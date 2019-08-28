DELIMITER //

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_create_message`(
    IN `queue_name` VARCHAR(50),
    IN `data` TEXT,
    IN `delay` INT
)
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
    SET @stm = CONCAT('
        INSERT INTO queue_', queue_name, '
            (data, exec_ts)
        VALUES
            (?, ADDDATE(NOW(), INTERVAL ', delay, ' SECOND))
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm USING data;
    DEALLOCATE PREPARE stm;
END//

-- #################

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_create_queue`(
    IN `queue_name` VARCHAR(50)

)
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
    SET @stm = CONCAT('
        CREATE TABLE `queue_', queue_name, '` (
            `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
            `data` TEXT NOT NULL,
            `connection_id` BIGINT(21) UNSIGNED NULL DEFAULT NULL,
            `attempts_num` TINYINT(3) UNSIGNED NOT NULL DEFAULT 0,
            `exec_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `created_ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE INDEX `connection_id` (`connection_id`),
            INDEX `attempts_num` (`attempts_num`),
            INDEX `exec_ts` (`exec_ts`)
        )COLLATE="utf8_general_ci" ENGINE=InnoDB
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @stm = CONCAT('
        CREATE TABLE `queue_', queue_name, '_dlq` (
            `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
            `data` TEXT NOT NULL,
            `created_ts` TIMESTAMP NOT NULL,
            PRIMARY KEY (`id`)
        )COLLATE="utf8_general_ci" ENGINE=InnoDB
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;
END//

-- #################

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_message`(
    IN `queue_name` VARCHAR(50)
)
LANGUAGE SQL
DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
    SET @stm = CONCAT('
        DELETE FROM queue_', queue_name, '
        WHERE connection_id = ', CONNECTION_ID()
    );
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @stm = CONCAT('
        UPDATE queue_', queue_name, '
        SET
            connection_id = NULL,
            exec_ts = ADDDATE(NOW(), INTERVAL attempts_num * 10 + 1 MINUTE),
            attempts_num = attempts_num + 1
        WHERE connection_id NOT IN (
            SELECT ID
            FROM information_schema.PROCESSLIST
            WHERE DB = DATABASE()
        )
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;


    SET @stm = CONCAT('
        INSERT INTO queue_', queue_name, '_dlq
            (data, created_ts)
            SELECT data, created_ts
            FROM queue_', queue_name, '
            WHERE attempts_num = 4
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @stm = CONCAT('
        DELETE FROM queue_', queue_name, '
        WHERE attempts_num = 4
    ');
    PREPARE stm FROM @stm;
    EXECUTE stm;
    DEALLOCATE PREPARE stm;

    SET @i = 0;
    waitmessage: LOOP
        SET @i = @i + 1;
        IF @i = 120 THEN
            LEAVE waitmessage;
        END IF ;

        SET @stm = CONCAT('
            UPDATE queue_', queue_name, '
            SET connection_id = ', CONNECTION_ID(), '
            WHERE
                connection_id IS NULL
                AND exec_ts < NOW()
            ORDER BY exec_ts ASC
            LIMIT 1
        ');
        PREPARE stm FROM @stm;
        EXECUTE stm;
        IF ROW_COUNT() = 1 THEN
            SET @stm = CONCAT('
                SELECT data
                FROM queue_', queue_name, '
                WHERE connection_id = ', CONNECTION_ID()
            );
            PREPARE stm FROM @stm;
            EXECUTE stm;
            DEALLOCATE PREPARE stm;
            LEAVE waitmessage;
        END IF;
        DEALLOCATE PREPARE stm;
        DO SLEEP(0.5);
    END LOOP waitmessage;
END//

-- #################

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_optimize_queue_table`(
    IN `queueName` VARCHAR(50)
)
LANGUAGE SQL
NOT DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
  SET @stm = CONCAT('OPTIMIZE TABLE ', 'queue_', queueName);
  PREPARE stm FROM @stm;
  EXECUTE stm;
  DEALLOCATE PREPARE stm;
END//

-- #################

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_queue_info`(
    IN `queueName` VARCHAR(50)
)
LANGUAGE SQL
NOT DETERMINISTIC
CONTAINS SQL
SQL SECURITY DEFINER
COMMENT ''
BEGIN
  SET @messages_num = null;
  SET @messages_dlq_num = null;
  SET @lag_time = null;
  SET @message = null;

  SET @queueTableName = CONCAT('queue_', queueName);

  SET @stm = CONCAT('SELECT COUNT(*) INTO @messages_num FROM ', @queueTableName);
  PREPARE stm FROM @stm;
  EXECUTE stm;
  DEALLOCATE PREPARE stm;

  SET @stm = CONCAT('SELECT COUNT(*) INTO @messages_dlq_num FROM ', @queueTableName,'_dlq');
  PREPARE stm FROM @stm;
  EXECUTE stm;
  DEALLOCATE PREPARE stm;

  SET @stm = CONCAT('SELECT data, TIMEDIFF(NOW(), exec_ts) INTO @message, @lag_time FROM ', @queueTableName,' ORDER BY exec_ts LIMIT 1');
  PREPARE stm FROM @stm;
  EXECUTE stm;
  DEALLOCATE PREPARE stm;

  SELECT queueName AS queueName, @messages_num AS messagesNum, @messages_dlq_num AS messagesDlqNum, @lag_time AS lagTime, @message AS message;
END//

DELIMITER ;
