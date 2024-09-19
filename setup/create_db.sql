/* 
  Run this script as MySQL/MariaDB root account.
  Make sure to change user_pass and admin_pass beforehand!
*/


/* Create database and tables */
CREATE DATABASE IF NOT EXISTS ;

USE tarxiv;

CREATE TABLE IF NOT EXISTS tns_entries(
	id INT,
	name VARCHAR(64),
	ra_hms VARCHAR(16),
	dec_hms VARCHAR(16),
	ra_deg FLOAT,
	dec_deg FLOAT,
	obj_type VARCHAR(16),
	host_name VARCHAR(64),
	host_redshift FLOAT,
	reporting_groups VARCHAR(256),
	discovery_data_sources VARCHAR(256), 
	classifying_groups VARCHAR(256),
	discovery_internal_name VARCHAR(128),
	public VARCHAR(4), 
	object_spectra INT,
	discovery_mag_flux FLOAT,
	discovery_discovery_filter FLOAT,
	discovery_datetime DATETIME,
	sender VARCHAR(64),
	PRIMARY KEY (id),
	INDEX USING BTREE (name)
);

/* Create users */
CREATE USER IF NOT EXISTS 'tarxiv_admin'@'localhost' IDENTIFIED BY 'admin_pass';
CREATE USER IF NOT EXISTS 'tarxiv_user'@'%' IDENTIFIED BY 'user_pass';

GRANT ALL PRIVILEGES ON tarxiv_admin.* TO 'tarxiv_admin'@'localhost';
GRANT GRANT SELECT, UPDATE, DELETE, INSERT ON tarxiv.* TO 'tarxiv_user'@'%';

FLUSH PRIVILEGES;

