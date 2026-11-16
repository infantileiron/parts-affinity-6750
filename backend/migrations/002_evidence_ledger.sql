-- 002_evidence_ledger.sql 备件包在线证据台账
-- 为每个备件包建立在签核后仍随作业持续更新的联用证据台账；
-- 贡献行记录每张作业对各包的当前贡献，支撑"撤销旧贡献再计新贡献"的增量维护与逐包对账。

ALTER TABLE `jobs` ADD COLUMN `revision` int DEFAULT 0;

CREATE TABLE `kit_ledgers` (
  `id` int NOT NULL AUTO_INCREMENT,
  `kit_id` int NOT NULL,
  `equipment` varchar(40) DEFAULT NULL COMMENT '来源批次的设备范围',
  `min_support` float DEFAULT NULL COMMENT '来源批次阈值',
  `min_confidence` float DEFAULT NULL,
  `min_lift` float DEFAULT NULL,
  `antecedent` json DEFAULT NULL COMMENT '来源规则前件',
  `consequent` json DEFAULT NULL COMMENT '来源规则后件',
  `state` varchar(20) DEFAULT 'building' COMMENT 'building重建中/ready可用/failed失败(旧版可读)',
  `valid_jobs` int DEFAULT 0 COMMENT '范围内有效作业数',
  `antecedent_count` int DEFAULT 0 COMMENT '含全部前件的作业数',
  `consequent_count` int DEFAULT 0 COMMENT '含全部后件的作业数',
  `both_count` int DEFAULT 0 COMMENT '前后件共同出现作业数',
  `support` float DEFAULT 0,
  `confidence` float DEFAULT 0,
  `lift` float DEFAULT 0,
  `verdict` varchar(20) DEFAULT 'below' COMMENT 'ok达标/below不达标',
  `baseline` json DEFAULT NULL COMMENT '签核时原证据快照，不再覆盖',
  `version` int DEFAULT 0 COMMENT '已完成重建代数',
  `processed` int DEFAULT 0 COMMENT '重建进度：已处理作业',
  `total` int DEFAULT 0 COMMENT '重建进度：作业总数',
  `error` text,
  `updated` varchar(40) DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `kit_id` (`kit_id`),
  CONSTRAINT `kit_ledgers_ibfk_1` FOREIGN KEY (`kit_id`) REFERENCES `kits` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `ledger_contribs` (
  `id` int NOT NULL AUTO_INCREMENT,
  `kit_id` int NOT NULL,
  `job_id` int NOT NULL,
  `job_revision` int DEFAULT 0 COMMENT '计入时作业的版本，过期状态跳过',
  `in_scope` int DEFAULT 0,
  `ant` int DEFAULT 0,
  `cons` int DEFAULT 0,
  `both` int DEFAULT 0,
  `fingerprint` varchar(64) DEFAULT NULL COMMENT '设备+用件指纹，幂等判定',
  PRIMARY KEY (`id`),
  UNIQUE KEY `kit_id` (`kit_id`,`job_id`),
  KEY `job_id` (`job_id`),
  CONSTRAINT `ledger_contribs_ibfk_1` FOREIGN KEY (`kit_id`) REFERENCES `kits` (`id`),
  CONSTRAINT `ledger_contribs_ibfk_2` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
