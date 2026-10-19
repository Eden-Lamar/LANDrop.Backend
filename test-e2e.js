// test-e2e.js
import { WebSocket } from 'ws';

const BASE_URL = 'http://localhost:3000';
const WS_URL = 'ws://localhost:3000/ws';

async function runTest() {
	console.log('🧪 Starting LanDrop E2E Stream Test...\n');

	// 1. Connect WebSockets
	const wsA = new WebSocket(WS_URL);
	const wsB = new WebSocket(WS_URL);

	await Promise.all([
		new Promise((resolve) => wsA.on('open', resolve)),
		new Promise((resolve) => wsB.on('open', resolve))
	]);

	// 2. Register Devices
	wsA.send(JSON.stringify({ type: 'register', id: 'device-a-12345', name: 'Sender PC' }));
	wsB.send(JSON.stringify({ type: 'register', id: 'device-b-67890', name: 'Receiver PC' }));

	let transferId = null;

	// 3. Handle Messages
	wsB.on('message', (data) => {
		const msg = JSON.parse(data.toString());
		if (msg.type === 'transfer:request') {
			transferId = msg.transferId;
			console.log(`📩 Receiver got request for transfer: ${transferId}`);

			// Accept Transfer
			wsB.send(JSON.stringify({ type: 'transfer:accept', transferId }));
		}
	});

	wsA.on('message', async (data) => {
		const msg = JSON.parse(data.toString());

		// Watch for the new progress events!
		if (msg.type === 'transfer:progress') {
			const mbps = (msg.speed / 1024 / 1024).toFixed(2);
			console.log(`📊 Progress: ${msg.progress}% | ${mbps} MB/s | ${msg.transferredBytes}/${msg.totalBytes} bytes`);
		}

		if (msg.type === 'transfer:ready') {
			console.log('⚡ Receiver HTTP connection ready! Starting POST stream from Sender...');

			// Generate a fake 50MB file filled with the letter 'A'
			const fake50MBFile = Buffer.alloc(50 * 1024 * 1024, 'A');

			const postRes = await fetch(`${BASE_URL}/transfers/${transferId}/data?deviceId=device-a-12345`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/octet-stream' },
				body: fake50MBFile
			});

			const json = await postRes.json();
			console.log('✅ POST Response:', json);
		}
	});

	// Small delay for registration to complete
	await new Promise((r) => setTimeout(r, 500));

	// 4. Sender initiates transfer
	console.log('📤 Sending transfer request...');
	wsA.send(JSON.stringify({
		type: 'transfer:request',
		receiverId: 'device-b-67890',
		filename: 'test.txt',
		size: 50 * 1024 * 1024
	}));

	// Wait for accept message, then Receiver opens GET request
	while (!transferId) await new Promise((r) => setTimeout(r, 100));
	await new Promise((r) => setTimeout(r, 500)); // allow ACCEPTED state to set

	console.log('📥 Receiver opening HTTP GET connection...');
	const getPromise = fetch(`${BASE_URL}/transfers/${transferId}/data?deviceId=device-b-67890`);

	const getRes = await getPromise;
	// Read it as a Blob so we can check the size instead of printing 50 million 'A's
	const blob = await getRes.blob();

	console.log(`🎉 Receiver downloaded content: "${blob.size}" bytes`);

	wsA.close();
	wsB.close();
}

runTest().catch(console.error);