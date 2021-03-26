export interface IKafkaMessageListener {
    onMessage(topic: string, partition: number, message: any): Promise<void>;
}
