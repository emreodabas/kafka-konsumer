package kafka

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkChunkMessages(b *testing.B) {
	b.ReportAllocs()
	rand.New(rand.NewSource(time.Now().UnixNano()))
	messages := generateMessages(10000, 100) // 10,000 messages, each 100 bytes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a copy of the messages slice to prevent compiler optimizations
		msgsCopy := make([]*Message, len(messages))
		copy(msgsCopy, messages)
		oldChunkMessages(&msgsCopy, 100, 10000)
	}
}

func BenchmarkChunkMessagesOptimized(b *testing.B) {
	b.ReportAllocs()
	rand.New(rand.NewSource(time.Now().UnixNano()))
	messages := generateMessages(10000, 100) // 10,000 messages, each 100 bytes

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a copy of the messages slice to prevent compiler optimizations
		msgsCopy := make([]*Message, len(messages))
		copy(msgsCopy, messages)
		chunkMessagesOptimized(msgsCopy, 100, 10000)
	}
}

func oldChunkMessages(allMessages *[]*Message, chunkSize int, chunkByteSize int) [][]*Message {
	var chunks [][]*Message

	allMessageList := *allMessages
	var currentChunk []*Message
	currentChunkSize := 0
	currentChunkBytes := 0

	for _, message := range allMessageList {
		messageByteSize := len(message.Value)

		// Check if adding this message would exceed either the chunk size or the byte size
		if len(currentChunk) >= chunkSize || (chunkByteSize != 0 && currentChunkBytes+messageByteSize > chunkByteSize) {
			// Avoid too low chunkByteSize
			if len(currentChunk) == 0 {
				panic("invalid chunk byte size, please increase it")
			}
			// If it does, finalize the current chunk and start a new one
			chunks = append(chunks, currentChunk)
			currentChunk = []*Message{}
			currentChunkSize = 0
			currentChunkBytes = 0
		}

		// Add the message to the current chunk
		currentChunk = append(currentChunk, message)
		currentChunkSize++
		currentChunkBytes += messageByteSize
	}

	// Add the last chunk if it has any messages
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	return chunks
}

func generateMessages(count int, valueSize int) []*Message {
	messages := make([]*Message, count)
	for i := 0; i < count; i++ {
		b := make([]byte, valueSize)
		for j := range b {
			b[j] = byte(rand.Intn(26) + 97)
		}
		messages[i] = &Message{Value: b}
	}
	return messages
}
