import db from './db.js';
import crypto from 'crypto';
import Ajv from 'ajv';

const ajv = new Ajv();

// In-memory map to hold active WebSocket connections
// Key: deviceId, Value: WebSocket instance
export const activeConnections = new Map();

// Define the schema for our first WebSocket event: device registration
const registerSchema = ajv.compile({
	type: 'object',
	required: ['type', 'id', 'name'],
	properties: {
		type: { type: 'string', const: 'register' },
		id: { type: 'string', minLength: 10 },
		name: { type: 'string', minLength: 1, maxLength: 50 }
	}
});

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
		socket.send(broadcastPayload);
	}
}

export default async function websocketRoutes(fastify, options) {
	fastify.get('/ws', { websocket: true }, (connection, req) => {
		const ws = connection;
		let currentDeviceId = null;

		ws.on('message', (message) => {
			try {
				const parsed = JSON.parse(message);

				// Handle Device Registration
				if (parsed.type === 'register') {
					if (!registerSchema(parsed)) {
						return ws.send(JSON.stringify({ type: 'error', message: 'Invalid payload' }));
					}

					// Use the client's persistent ID
					currentDeviceId = parsed.id;

					// Upsert the device into SQLite
					const stmt = db.prepare(`
            INSERT INTO devices (id, name, ip_address, last_seen)
            VALUES (@id, @name, @ip, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET last_seen = CURRENT_TIMESTAMP, name = @name
          `);

					stmt.run({
						id: currentDeviceId,
						name: parsed.name,
						ip: req.ip // Fastify provides the connecting IP
					});

					// Add to our active connections map
					activeConnections.set(currentDeviceId, ws);

					fastify.log.info(`📱 Device registered: ${parsed.name} (${currentDeviceId})`);

					// Tell the new client what their ID is
					ws.send(JSON.stringify({ type: 'registered', id: currentDeviceId }));

					// Broadcast the updated list to everyone
					broadcastDeviceList();

				} else {
					// UPDATE: Trap unknown event types to help debug the client
					ws.send(JSON.stringify({
						type: 'error',
						message: `Unknown event type: ${parsed.type}`
					}));
				}

			} catch (err) {
				ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON format' }));
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