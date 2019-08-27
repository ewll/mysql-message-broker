<?php namespace Ewll\MysqlMessageBrokerBundle;

use Ewll\DBBundle\DB\Client;

class MessageBroker
{
    private $dbClient;

    public function __construct(Client $dbClient)
    {
        $this->dbClient = $dbClient;
    }

    public function getMessage(string $queue): array
    {
        while (1) {
            $statement = $this->dbClient
                ->prepare("CALL sp_get_message('$queue')")
                ->execute();

            $data = $statement->fetchColumn();
            if (null === $data) {
                continue;
            }
            $data = json_decode($data, true);

            break;
        }

        return $data;
    }

    public function createMessage(string $queue, array $data, int $delay = 0): void
    {
        $this->dbClient
            ->prepare("CALL sp_create_message('$queue', :message, :delay)")
            ->execute([
                'message' => json_encode($data),
                'delay' => $delay,
            ]);
    }

    public function optimizeQueueTable(string $queueName): void
    {
        $this->dbClient
            ->prepare("CALL sp_optimize_queue_table(:queueName)")
            ->execute([
                'queueName' => $queueName,
            ]);
    }

    public function getQueueInfo(string $queueName): array
    {
        return $this->dbClient
            ->prepare("CALL sp_queue_info(:queueName)")
            ->execute(['queueName' => $queueName])
            ->fetchArray();
    }
}
