import { IKafkaMessageListener } from "./IKafkaMessageListener";

export class KafkaSubscription {
    public topic: string;
    public groupId: string;
    public options: any;
    public handler: any;
    public listener: IKafkaMessageListener;
}