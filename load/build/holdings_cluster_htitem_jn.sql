CREATE TABLE `holdings_cluster_htitem_jn` (
  `cluster_id` bigint(20) NOT NULL,
  `volume_id` varchar(50) NOT NULL,
  PRIMARY KEY (`cluster_id`,`volume_id`),
  KEY `cluster_htitem_jn_volume_id_index` (`volume_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1
