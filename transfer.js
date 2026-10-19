import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import crypto from 'node:crypto';
import db from './db.js';
import { activeConnections } from './ws.js';

// Holds waiting HTTP GET responses until the matching POST arrives
// Key: transferId, Value: { req, reply, receiverId }
const pendingDownloads = new Map();

// Helper: Send WebSocket events safely
function notifyDevice(deviceId, payload) {
	const socket = activeConnections.get(deviceId);
	if (socket && socket.readyState === 1) {
		socket.send(JSON.stringify(payload));
	}
}

// Helper: Create an inline SHA-256 hashing stream
function createHashTransform() {
	const hash = crypto.createHash('sha256');
	const transform = new Transform({
		transform(chunk, encoding, callback) {
			hash.update(chunk);
			callback(null, chunk); // Pass the chunk through unmodified
		}
	});
	return { transform, getDigest: () => hash.digest('hex') };
}

// Progress tracking stream
function createProgressTransform(transferId, totalSize, senderId, receiverId) {
	let transferredBytes = 0;
	let lastUpdateTime = Date.now();
	let lastUpdateBytes = 0;
	let isFirstChunk = true;

	return new Transform({
		transform(chunk, encoding, callback) {
			transferredBytes += chunk.length;

			const now = Date.now();
			const timeDiff = now - lastUpdateTime;

			// Throttle updates: Fire on the first chunk, and then only every 500ms
			if (isFirstChunk || timeDiff >= 500) {
				const bytesSinceLast = transferredBytes - lastUpdateBytes;

				// Calculate Bytes per Second
				const speedBps = timeDiff > 0 ? Math.round((bytesSinceLast / timeDiff) * 1000) : 0;

				const payload = {
					type: 'transfer:progress',
					transferId,
					transferredBytes,
					totalBytes: totalSize,
					speed: speedBps, // Send raw bytes/sec; the frontend will format it to MB/s
					progress: totalSize > 0 ? parseFloat(((transferredBytes / totalSize) * 100).toFixed(1)) : 0
				};

				notifyDevice(senderId, payload);
				notifyDevice(receiverId, payload);

				lastUpdateTime = now;
				lastUpdateBytes = transferredBytes;
				isFirstChunk = false;
			}

			callback(null, chunk);
		}
	});
}

export default async function transferRoutes(fastify, options) {

	/**
	 * 1. RECEIVER CONNECTION: GET /transfers/:id/data
	 * Receiver connects first, validates transfer, and waits for POST stream.
	 */
	fastify.get('/transfers/:id/data', async (request, reply) => {
		const transferId = request.params.id;
		const deviceId = request.headers['x-device-id'] || request.query.deviceId;

		if (!deviceId) {
			return reply.code(401).send({ error: 'Device identity required (x-device-id header or deviceId query param)' });
		}

		// 1. Verify transfer existence & authorization
		const transfer = db.prepare('SELECT * FROM transfers WHERE id = ?').get(transferId);
		if (!transfer) {
			return reply.code(404).send({ error: 'Transfer not found' });
		}

		if (transfer.receiver_id !== deviceId) {
			return reply.code(403).send({ error: 'Unauthorized: You are not the designated receiver' });
		}

		if (transfer.status !== 'ACCEPTED') {
			return reply.code(400).send({ error: `Transfer is not in ACCEPTED state. Current state: ${transfer.status}` });
		}

		if (pendingDownloads.has(transferId)) {
			return reply.code(409).send({ error: 'A receiver connection is already waiting for this transfer' });
		}

		// 2. Set response headers for binary download
		reply.raw.writeHead(200, {
			'Content-Type': 'application/octet-stream',
			'Content-Length': transfer.size,
			'Content-Disposition': `attachment; filename="${encodeURIComponent(transfer.filename)}"`,
			'Cache-Control': 'no-cache'
		});

		// 3. Register holding request & transition status
		pendingDownloads.set(transferId, {
			req: request,
			reply: reply,
			receiverId: deviceId
		});

		db.prepare("UPDATE transfers SET status = 'RECEIVER_READY' WHERE id = ?").run(transferId);

		// Notify sender over WebSocket that the receiver's HTTP connection is live
		notifyDevice(transfer.sender_id, {
			type: 'transfer:ready',
			transferId
		});

		fastify.log.info(`📥 Receiver connected and waiting for HTTP GET stream (Transfer: ${transferId})`);

		// Handle client abort / disconnect while waiting
		request.raw.on('close', () => {
			if (pendingDownloads.has(transferId)) {
				const currentTransfer = db.prepare('SELECT status FROM transfers WHERE id = ?').get(transferId);
				if (currentTransfer && currentTransfer.status === 'RECEIVER_READY') {
					pendingDownloads.delete(transferId);
					db.prepare("UPDATE transfers SET status = 'CANCELLED' WHERE id = ?").run(transferId);
					notifyDevice(transfer.sender_id, { type: 'transfer:cancelled', transferId });
					fastify.log.info(`❌ Receiver aborted GET request prior to POST stream (Transfer: ${transferId})`);
				}
			}
		});

		// Hijack response handling (Fastify will not send a separate payload body)
		return reply.hijack();
	});

	/**
	 * 2. SENDER CONNECTION: POST /transfers/:id/data
	 * Sender pushes raw binary stream; server pipes directly to waiting GET.
	 */
	fastify.post('/transfers/:id/data', async (request, reply) => {
		const transferId = request.params.id;
		const deviceId = request.headers['x-device-id'] || request.query.deviceId;

		if (!deviceId) {
			return reply.code(401).send({ error: 'Device identity required (x-device-id header or deviceId query param)' });
		}

		// 1. Enforce 6-Point State Guard
		const transfer = db.prepare('SELECT * FROM transfers WHERE id = ?').get(transferId);
		if (!transfer) {
			return reply.code(404).send({ error: 'Transfer not found' });
		}

		if (transfer.sender_id !== deviceId) {
			return reply.code(403).send({ error: 'Unauthorized: You are not the designated sender' });
		}

		if (transfer.status !== 'RECEIVER_READY') {
			return reply.code(400).send({ error: `Transfer is not ready for streaming. Current status: ${transfer.status}` });
		}

		const pendingDownload = pendingDownloads.get(transferId);
		if (!pendingDownload) {
			return reply.code(400).send({ error: 'Receiver GET connection is not actively waiting' });
		}

		// 2. Mark DB status as TRANSFERRING
		db.prepare("UPDATE transfers SET status = 'TRANSFERRING' WHERE id = ?").run(transferId);

		notifyDevice(transfer.sender_id, { type: 'transfer:transferring', transferId });
		notifyDevice(transfer.receiver_id, { type: 'transfer:transferring', transferId });

		fastify.log.info(`🚀 Starting streaming pipeline for transfer: ${transferId}`);

		// Initialize both transform streams
		const progressTransform = createProgressTransform(transferId, transfer.size, transfer.sender_id, transfer.receiver_id);
		// 3. Create Hash Transform & Pipe Streams directly
		const { transform: hashTransform, getDigest } = createHashTransform();

		try {
			// stream.pipeline automatically propagates backpressure across Node streams
			await pipeline(
				request.raw,               // Incoming POST stream
				progressTransform,         // Calculates & emits progress
				hashTransform,             // Inline SHA-256 calculation
				pendingDownload.reply.raw  // Outgoing GET stream
			);

			// 4. Post-Transfer Verification
			const calculatedHash = getDigest();
			fastify.log.info(`🔍 Stream finished. Calculated SHA-256: ${calculatedHash}`);

			db.prepare("UPDATE transfers SET status = 'VERIFYING' WHERE id = ?").run(transferId);

			let finalStatus = 'COMPLETED';
			if (transfer.expected_hash && transfer.expected_hash.toLowerCase() !== calculatedHash.toLowerCase()) {
				finalStatus = 'CORRUPTED';
				fastify.log.error(`⚠️ Hash Mismatch for transfer ${transferId}! Expected: ${transfer.expected_hash}, Calculated: ${calculatedHash}`);
			}

			db.prepare('UPDATE transfers SET status = ? WHERE id = ?').run(finalStatus, transferId);

			// Clean up waiting request record
			pendingDownloads.delete(transferId);

			// 5. Notify both clients over WebSocket
			const completionPayload = {
				type: finalStatus === 'COMPLETED' ? 'transfer:completed' : 'transfer:corrupted',
				transferId,
				sha256: calculatedHash
			};

			notifyDevice(transfer.sender_id, completionPayload);
			notifyDevice(transfer.receiver_id, completionPayload);

			return reply.code(200).send({
				status: finalStatus,
				bytesTransferred: transfer.size,
				sha256: calculatedHash
			});

		} catch (err) {
			fastify.log.error(`💥 Pipeline failure on transfer ${transferId}:`, err);

			pendingDownloads.delete(transferId);
			db.prepare("UPDATE transfers SET status = 'FAILED' WHERE id = ?").run(transferId);

			const failurePayload = { type: 'transfer:failed', transferId, reason: 'Stream pipeline error' };
			notifyDevice(transfer.sender_id, failurePayload);
			notifyDevice(transfer.receiver_id, failurePayload);

			return reply.code(500).send({ error: 'File transfer failed during streaming' });
		}
	});
}