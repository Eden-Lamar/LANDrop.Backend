import Fastify from 'fastify';
import cors from '@fastify/cors';
import websocket from '@fastify/websocket';
import { Bonjour } from 'bonjour-service';
import db from './db.js'; // Importing this ensures the tables are created on startup
import websocketRoutes from './ws.js';

// Initialize Fastify with pretty logging
const fastify = Fastify({
	logger: {
		transport: {
			target: 'pino-pretty', // Makes logs readable in the terminal
			options: { translateTime: 'HH:MM:ss Z', ignore: 'pid,hostname' }
		}
	}
});

// Register plugins
await fastify.register(cors, { origin: '*' }); // Allows our React app to connect
await fastify.register(websocket); // Enables WebSocket support

// Register our new WebSocket routes
await fastify.register(websocketRoutes);

// Initialize Bonjour (mDNS)
const bonjour = new Bonjour();

// A simple health check route to test if HTTP is working
fastify.get('/ping', async (request, reply) => {
	return { status: 'LanDrop server is running', timestamp: new Date() };
});

// Ensure we gracefully stop the mDNS broadcast when the server shuts down
fastify.addHook('onClose', (instance, done) => {
	bonjour.destroy();
	done();
});

// Start the server
const start = async () => {
	try {
		// Listen on all network interfaces (0.0.0.0) so other devices on Wi-Fi can connect
		await fastify.listen({ port: 3000, host: '0.0.0.0' });
		fastify.log.info('🚀 LanDrop server is ready to accept connections');

		// Broadcast the LanDrop service over the local network
		// Mobile phones and other PCs will look for '_landrop._tcp'
		bonjour.publish({
			name: 'LanDrop Server',
			type: 'landrop',
			protocol: 'tcp',
			port: 3000
		});

		fastify.log.info('📡 mDNS broadcasting on _landrop._tcp.local');

	} catch (err) {
		fastify.log.error(err);
		process.exit(1);
	}
};

start();