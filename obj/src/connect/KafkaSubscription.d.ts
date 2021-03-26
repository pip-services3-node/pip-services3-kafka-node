import { IKafkaMessageListener } from "./IKafkaMessageListener";
export declare class KafkaSubscription {
    topic: string;
    groupId: string;
    options: any;
    handler: any;
    listener: IKafkaMessageListener;
}
