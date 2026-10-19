import db from './db.js';
import crypto from 'crypto';
import Ajv from 'ajv';

const ajv = new Ajv();

// In-memory map to hold active WebSocket connections
// Key: deviceId, Value: WebSocket instance
export const activeConnections = new Map();

// Format Ajv errors into a clean string
function formatAjvError(validator) {
	if (!validator.errors || validator.errors.length === 0) return 'Invalid payload';
	const err = validator.errors[0];
	return `${err.instancePath || 'payload'} ${err.message}`;
}

// Define the schema for our first WebSocket event: device registration
const registerSchema = ajv.compile({
	type: 'object',
	required: ['type', 'id', 'name'],
	additionalProperties: false,
	properties: {
		type: { type: 'string', const: 'register' },
		id: { type: 'string', minLength: 10 },
		name: { type: 'string', minLength: 1, maxLength: 50 }
	}
});

const transferRequestSchema = ajv.compile({
	type: 'object',
	required: ['type', 'receiverId', 'filename', 'size'],
	additionalProperties: false,
	properties: {
		type: { type: 'string', const: 'transfer:request' },
		receiverId: { type: 'string', minLength: 10 },
		filename: { type: 'string', minLength: 1, maxLength: 255 },
		size: { type: 'integer', minimum: 0 },
		sha256: { type: 'string', pattern: '^[a-fA-F0-9]{64}$' } // Optional for now
	}
});

const transferResponseSchema = ajv.compile({
	type: 'object',
	required: ['type', 'transferId'],
	additionalProperties: false,
	properties: {
		type: { type: 'string', enum: ['transfer:accept', 'transfer:reject', 'transfer:ready'] },
		transferId: { type: 'string' }
	}
});

const clipboardSendSchema = ajv.compile({
	type: 'object',
	required: ['type', 'receiverId', 'content'],
	additionalProperties: false,
	properties: {
		type: { type: 'string', const: 'clipboard:send' },
		receiverId: { type: 'string', minLength: 10 },
		content: { type: 'string', minLength: 1, maxLength: 50000 }
	}
});

// Explicit mapping to keep the WebSocket protocol clean
const eventByStatus = {
	ACCEPTED: 'transfer:accepted',
	REJECTED: 'transfer:rejected',
	RECEIVER_READY: 'transfer:ready'
};

// Helper function to broadcast the list of online devices
function broadcastDeviceList() {
	// Get all currently connected device IDs
	const onlineIds = Array.from(activeConnections.keys());

	// If no devices are online, there's nothing to broadcast
	if (onlineIds.length === 0) return;

	// Fetch their details from SQLite
	const placeholders = onlineIds.map(() => '?').join(',');
	const query = `SELECT id, name, ip_address FROM devices WHERE id IN (${placeholders})`;
	const onlineDevices = db.prepare(query).all(...onlineIds);

	const broadcastPayload = JSON.stringify({
		type: 'device-list',
		devices: onlineDevices
	});

	// Send the updated list to every connected client
	for (const socket of activeConnections.values()) {
		// 1 represents WebSocket.OPEN state
		if (socket.readyState === 1) {
			socket.send(broadcastPayload);
		}
	}
}

export default async function websocketRoutes(fastify, options) {
	fastify.get('/ws', { websocket: true }, (connection, req) => {
		const ws = connection;
		let currentDeviceId = null;

		ws.on('message', (message) => {
			let parsed;

			// 1. Isolate JSON parsing
			try {
				parsed = JSON.parse(message.toString());
			} catch {
				return ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON format' }));
			}

			// Guard against null, arrays, or primitive values
			if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
				return ws.send(JSON.stringify({ type: 'error', message: 'Message must be a JSON object' }));
			}

			try {
				// 2. Guard: Must be registered
				if (parsed.type !== 'register' && !currentDeviceId) {
					return ws.send(JSON.stringify({ type: 'error', message: 'Register your device before sending other messages' }));
				}

				// Handle Device Registration
				if (parsed.type === 'register') {
					if (currentDeviceId) return ws.send(JSON.stringify({ type: 'error', message: 'Device already registered on this connection' }));
					if (!registerSchema(parsed)) return ws.send(JSON.stringify({ type: 'error', message: formatAjvError(registerSchema) }));

					// Duplicate connection guard
					const existingSocket = activeConnections.get(parsed.id);
					if (existingSocket && existingSocket.readyState === 1) {
						return ws.send(JSON.stringify({ type: 'error', message: 'This device is already connected' }));
					}

					currentDeviceId = parsed.id;

					const stmt = db.prepare(`
            INSERT INTO devices (id, name, ip_address, last_seen)
            VALUES (@id, @name, @ip, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET last_seen = CURRENT_TIMESTAMP, name = @name
          `);

					stmt.run({ id: currentDeviceId, name: parsed.name, ip: req.ip });

					activeConnections.set(currentDeviceId, ws);
					fastify.log.info(`📱 Device registered: ${parsed.name} (${currentDeviceId})`);

					ws.send(JSON.stringify({ type: 'registered', id: currentDeviceId }));
					broadcastDeviceList();

				} else if (parsed.type === 'transfer:request') {
					if (!transferRequestSchema(parsed)) return ws.send(JSON.stringify({ type: 'error', message: formatAjvError(transferRequestSchema) }));

					// Guard: Prevent sending to self
					if (parsed.receiverId === currentDeviceId) {
						return ws.send(JSON.stringify({ type: 'error', message: 'You cannot send a transfer to yourself' }));
					}

					const receiverSocket = activeConnections.get(parsed.receiverId);
					if (!receiverSocket) return ws.send(JSON.stringify({ type: 'error', message: 'Receiver is offline' }));

					// 1. Generate transfer ID
					const transferId = crypto.randomUUID();

					// 2. Log it as PENDING in the state machine
					const stmt = db.prepare(`
            INSERT INTO transfers (id, sender_id, receiver_id, filename, size, status, expected_hash)
            VALUES (@id, @sender, @receiver, @filename, @size, 'PENDING', @hash)
          `);

					stmt.run({
						id: transferId,
						sender: currentDeviceId,
						receiver: parsed.receiverId,
						filename: parsed.filename,
						size: parsed.size,
						hash: parsed.sha256 || null
					});

					// Notify SENDER
					ws.send(JSON.stringify({
						type: 'transfer:pending',
						transferId,
						receiverId: parsed.receiverId,
						filename: parsed.filename,
						size: parsed.size
					}));

					// Notify RECEIVER
					receiverSocket.send(JSON.stringify({
						type: 'transfer:request',
						transferId,
						senderId: currentDeviceId,
						filename: parsed.filename,
						size: parsed.size,
						sha256: parsed.sha256 || null
					}));

				} else if (['transfer:accept', 'transfer:reject', 'transfer:ready'].includes(parsed.type)) {
					if (!transferResponseSchema(parsed)) return ws.send(JSON.stringify({ type: 'error', message: formatAjvError(transferResponseSchema) }));

					// 3. Strict State Transitions
					let result;
					let targetStatus;

					if (parsed.type === 'transfer:accept') {
						targetStatus = 'ACCEPTED';
						result = db.prepare(`UPDATE transfers SET status = 'ACCEPTED' WHERE id = ? AND receiver_id = ? AND status = 'PENDING'`).run(parsed.transferId, currentDeviceId);
					} else if (parsed.type === 'transfer:reject') {
						targetStatus = 'REJECTED';
						result = db.prepare(`UPDATE transfers SET status = 'REJECTED' WHERE id = ? AND receiver_id = ? AND status = 'PENDING'`).run(parsed.transferId, currentDeviceId);
					} else if (parsed.type === 'transfer:ready') {
						targetStatus = 'RECEIVER_READY';
						result = db.prepare(`UPDATE transfers SET status = 'RECEIVER_READY' WHERE id = ? AND receiver_id = ? AND status = 'ACCEPTED'`).run(parsed.transferId, currentDeviceId);
					}

					if (result.changes === 0) {
						return ws.send(JSON.stringify({ type: 'error', message: 'Invalid state transition or unauthorized' }));
					}

					// Notify sender of the state change
					const transfer = db.prepare('SELECT sender_id FROM transfers WHERE id = ?').get(parsed.transferId);
					const senderSocket = activeConnections.get(transfer.sender_id);
					if (senderSocket) {
						senderSocket.send(JSON.stringify({ type: eventByStatus[targetStatus], transferId: parsed.transferId }));
					}

				} else if (parsed.type === 'clipboard:send') {
					if (!clipboardSendSchema(parsed)) return ws.send(JSON.stringify({ type: 'error', message: formatAjvError(clipboardSendSchema) }));

					if (parsed.receiverId === currentDeviceId) {
						return ws.send(JSON.stringify({ type: 'error', message: 'You cannot send clipboard data to yourself' }));
					}

					const receiverSocket = activeConnections.get(parsed.receiverId);
					if (!receiverSocket || receiverSocket.readyState !== 1) {
						return ws.send(JSON.stringify({ type: 'error', message: 'Receiver is offline' }));
					}

					// 1. Forward the clipboard data to the receiver
					receiverSocket.send(JSON.stringify({
						type: 'clipboard:receive',
						senderId: currentDeviceId,
						content: parsed.content,
						timestamp: new Date().toISOString()
					}));

					// 2. Send an immediate acknowledgment back to the sender
					ws.send(JSON.stringify({
						type: 'clipboard:sent',
						receiverId: parsed.receiverId
					}));

				} else {
					// UPDATE: Trap unknown event types to help debug the client
					ws.send(JSON.stringify({
						type: 'error',
						message: `Unknown event type: ${parsed.type}`
					}));
				}

			} catch (err) {
				fastify.log.error(err);
				ws.send(JSON.stringify({ type: 'error', message: 'Internal server error' }));
			}
		});

		// Handle disconnects
		ws.on('close', () => {
			// UPDATE: Concurrency check. Only delete if THIS socket is still the active one
			if (currentDeviceId && activeConnections.get(currentDeviceId) === ws) {
				activeConnections.delete(currentDeviceId);
				fastify.log.info(`🔌 Device disconnected: ${currentDeviceId}`);
				broadcastDeviceList();
			}
		});
	});
}