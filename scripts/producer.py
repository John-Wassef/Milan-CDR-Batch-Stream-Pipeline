import time
import glob
from kafka import KafkaProducer

def produce_messages(raw_path="/opt/spark/data/raw/*.txt", max_lines=5000):
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip'
    )
    
    files = glob.glob(raw_path)
    if not files:
        print("No files found!")
        return
    
    file_path = files[0]
    print(f"Streaming single file: {file_path}")
    print(f"Limited to {max_lines} lines for quick demo...")
    
    total_messages = 0
    batch = []
    batch_size = 100
    
    with open(file_path, "r") as f:
        for line_num, line in enumerate(f):
            if line_num >= max_lines:
                break
                
            batch.append(line.strip().encode("utf-8"))
            total_messages += 1
            
            if len(batch) >= batch_size:
                for msg in batch:
                    producer.send("telecom_events", value=msg)
                batch = []
                time.sleep(0.01)
                print(f"Sent {total_messages} messages so far...")
    
    for msg in batch:
        producer.send("telecom_events", value=msg)
    
    producer.flush()
    print(f"Producer finished! Total messages sent: {total_messages}")

if __name__ == "__main__":
    import sys
    raw_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/raw/*.txt"
    produce_messages(raw_path)