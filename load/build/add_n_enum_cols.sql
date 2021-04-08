-- Adding n_enum to the ETAS production tables, so that we can be more granular and
-- accurate in granting access to mpms.
-- Corresponding changes made to copy_htid_clusterid.rb.
ALTER TABLE holdings_cluster_htitem_jn_tmp ADD n_enum VARCHAR(60) NULL DEFAULT NULL;
ALTER TABLE holdings_cluster_htitem_jn     ADD n_enum VARCHAR(60) NULL DEFAULT NULL;
