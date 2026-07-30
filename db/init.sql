-- Minimal init SQL for local development
-- Creates minimal tables used by monitor/dispatcher and seeds fallback config
CREATE DATABASE IF NOT EXISTS `crawler_db` DEFAULT CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_general_ci';
USE `crawler_db`;

-- Config table (minimal)
CREATE TABLE IF NOT EXISTS `ASADAL_CRAWLING_CONFIG` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `chat_bot_id` VARCHAR(255) DEFAULT NULL,
  `key` VARCHAR(100) NOT NULL,
  `value` TEXT,
  UNIQUE KEY `ux_config_chat_key` (`chat_bot_id`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Crawling log (minimal)
CREATE TABLE IF NOT EXISTS `ASADAL_CRAWLING_LOG` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `job_id` VARCHAR(128) NOT NULL,
  `scan` INT DEFAULT 0,
  `collection` INT DEFAULT 0,
  `save` INT DEFAULT 0,
  `study` INT DEFAULT 0,
  `pages` INT DEFAULT 0,
  `status` VARCHAR(64) DEFAULT NULL,
  `colle` VARCHAR(64) DEFAULT NULL,
  `start_at` DATETIME DEFAULT NULL,
  `end_at` DATETIME DEFAULT NULL,
  INDEX (`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed fallback config (default)
INSERT IGNORE INTO `ASADAL_CRAWLING_CONFIG` (`chat_bot_id`, `key`, `value`) VALUES
(NULL, 'week_count', '300'),
(NULL, 'page_count', '100'),
(NULL, 'stop_count', '1000');

