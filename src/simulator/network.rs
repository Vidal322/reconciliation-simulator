use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessagePayload {
    /// Upward contribution of a local or partially aggregated exact sketch.
    RibltSketchChunk {
        sketch_cells: usize,
        contributors: Vec<usize>,
    },

    /// Downward broadcast of a combined/global exact sketch.
    CombinedRibltSketchChunk {
        sketch_cells: usize,
        contributors: Vec<usize>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub from: usize,
    pub to: usize,
    pub payload: MessagePayload,
    pub bytes: usize,
}

impl Message {
    pub fn new(from: usize, to: usize, payload: MessagePayload, bytes: usize) -> Self {
        Self {
            from,
            to,
            payload,
            bytes,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Network {
    inboxes: HashMap<usize, Vec<Message>>,
    total_messages: usize,
    total_bytes: usize,
}

impl Network {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enqueue one message for delivery to its destination node.
    pub fn send(&mut self, message: Message) {
        self.total_messages += 1;
        self.total_bytes += message.bytes;

        self.inboxes.entry(message.to).or_default().push(message);
    }

    /// Enqueue many messages.
    pub fn send_many<I>(&mut self, messages: I)
    where
        I: IntoIterator<Item = Message>,
    {
        for message in messages {
            self.send(message);
        }
    }

    /// Drains all messages currently waiting for a given node.
    pub fn drain_inbox(&mut self, node_id: usize) -> Vec<Message> {
        self.inboxes.remove(&node_id).unwrap_or_default()
    }

    /// Returns the number of queued messages for a given node.
    pub fn inbox_len(&self, node_id: usize) -> usize {
        self.inboxes.get(&node_id).map_or(0, Vec::len)
    }

    /// Returns true if there are no queued messages anywhere.
    pub fn is_empty(&self) -> bool {
        self.inboxes.values().all(Vec::is_empty)
    }

    /// Total number of messages ever sent through this network instance.
    pub fn total_messages(&self) -> usize {
        self.total_messages
    }

    /// Total number of bytes ever sent through this network instance.
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Clears all pending messages and resets counters.
    pub fn reset(&mut self) {
        self.inboxes.clear();
        self.total_messages = 0;
        self.total_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_and_drain_single_message() {
        let mut network = Network::new();

        let msg = Message::new(
            1,
            0,
            MessagePayload::RibltSketchChunk {
                sketch_cells: 8,
                contributors: vec![1],
            },
            128,
        );

        network.send(msg.clone());

        assert_eq!(network.total_messages(), 1);
        assert_eq!(network.total_bytes(), 128);
        assert_eq!(network.inbox_len(0), 1);

        let inbox = network.drain_inbox(0);
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0], msg);
        assert_eq!(network.inbox_len(0), 0);
    }

    #[test]
    fn send_many_messages() {
        let mut network = Network::new();

        network.send_many(vec![
            Message::new(
                1,
                0,
                MessagePayload::RibltSketchChunk {
                    sketch_cells: 4,
                    contributors: vec![1],
                },
                64,
            ),
            Message::new(
                2,
                0,
                MessagePayload::RibltSketchChunk {
                    sketch_cells: 4,
                    contributors: vec![2],
                },
                64,
            ),
        ]);

        assert_eq!(network.total_messages(), 2);
        assert_eq!(network.total_bytes(), 128);
        assert_eq!(network.inbox_len(0), 2);
    }

    #[test]
    fn network_empty_state_changes_correctly() {
        let mut network = Network::new();
        assert!(network.is_empty());

        network.send(Message::new(
            1,
            0,
            MessagePayload::CombinedRibltSketchChunk {
                sketch_cells: 16,
                contributors: vec![0, 1, 2],
            },
            256,
        ));

        assert!(!network.is_empty());

        let _ = network.drain_inbox(0);
        assert!(network.is_empty());
    }
}
