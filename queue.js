class Queue {
	// {
	//  A: ["message1", "message2"],
	//  B: ["message3", "message4"],
	//  C: [],
// {
	//  A: [callback func 1, "message2"],
	//  B: ["message3", "message4"],
	//  C: [],
	messages = new Map();
	subscribers = new Map();

	constructor() {}

	generateMessage = (idx, message) => {
		return `IDX=${idx},MSG=${message}`;
	};

	register = (topicId, idx, callback) => {
		if (!this.subscribers.has(topicId)) {
			this.subscribers.set(topicId, [callback]);
		} else {
			this.subscribers.get(topicId).push(callback);
		}

		console.log('IDX:', idx);
		const messages = this.messages.get(topicId) ?? [];
		console.log('messages', messages);
		for (let messageIdx = idx; messageIdx < messages.length; messageIdx++) {
			callback(this.generateMessage(messageIdx, messages[messageIdx]));
		}
	};

	unregister = (topicId, callback) => {
		if (!this.subscribers.has(topicId)) {
			return;
		}
		this.subscribers.set(
			topicId,
			this.subscribers.get(topicId).filter(cb => cb !== callback)
		);
	};

	publish = (topicId, message) => {
		const messageToTopic = this.messages.get(topicId);
		if (messageToTopic) {
			messageToTopic.push(message);
		} else {
			this.messages.set(topicId, [message]);
		}
		for (const callback of this.subscribers.get(topicId)) {
			try {
				const idx = this.messages.get(topicId).length;
				callback(this.generateMessage(idx, message));
			} catch (error) {
				console.error('Error in subscriber callback:', error);
				this.unregister(topicId, callback);
			}
		}
	};
}

export default Queue;
