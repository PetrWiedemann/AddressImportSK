/*
SQLyog Community v12.5.1 (64 bit)
MySQL - 5.7.19-log : Database - adresy
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`adresy` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin */;

USE `adresy`;

/*Table structure for table `sk_cities` */

DROP TABLE IF EXISTS `sk_cities`;

CREATE TABLE `sk_cities` (
  `ID` int(11) NOT NULL,
  `CITY` varchar(255) COLLATE utf8_bin NOT NULL,
  `FT` varchar(255) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`ID`),
  FULLTEXT KEY `FT` (`FT`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*Table structure for table `sk_city_parts` */

DROP TABLE IF EXISTS `sk_city_parts`;

CREATE TABLE `sk_city_parts` (
  `ID` int(11) NOT NULL,
  `CITY_PART` varchar(255) COLLATE utf8_bin NOT NULL,
  `FT` varchar(255) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`ID`),
  FULLTEXT KEY `FT` (`FT`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*Table structure for table `sk_places` */

DROP TABLE IF EXISTS `sk_places`;

CREATE TABLE `sk_places` (
  `ID` int(11) NOT NULL,
  `STREET1` varchar(255) COLLATE utf8_bin NOT NULL,
  `STREET2` varchar(255) COLLATE utf8_bin NOT NULL,
  `CITY` varchar(255) COLLATE utf8_bin NOT NULL,
  `ZIP` varchar(10) COLLATE utf8_bin NOT NULL,
  `FT` varchar(255) COLLATE utf8_bin NOT NULL,
  `LON` decimal(15,7) NOT NULL DEFAULT '0.0000000',
  `LAT` decimal(15,7) NOT NULL DEFAULT '0.0000000',
  `FT_STREET` varchar(255) COLLATE utf8_bin NOT NULL,
  `FT_CITY` varchar(255) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`ID`),
  FULLTEXT KEY `FT` (`FT`),
  FULLTEXT KEY `FT_STREET` (`FT_STREET`),
  FULLTEXT KEY `FT_CITY` (`FT_CITY`),
  FULLTEXT KEY `FT_ZIP` (`ZIP`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*Table structure for table `sk_relations` */

DROP TABLE IF EXISTS `sk_relations`;

CREATE TABLE `sk_relations` (
  `OBJ_ID` int(11) NOT NULL,
  `PLACE_ID` int(11) NOT NULL,
  UNIQUE KEY `OBJ_TO_PLACE` (`OBJ_ID`,`PLACE_ID`),
  KEY `OBJ_ID` (`OBJ_ID`),
  KEY `PLACE_ID` (`PLACE_ID`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*Table structure for table `sk_streets` */

DROP TABLE IF EXISTS `sk_streets`;

CREATE TABLE `sk_streets` (
  `ID` int(11) NOT NULL,
  `STREET` varchar(255) COLLATE utf8_bin NOT NULL,
  `FT` varchar(255) COLLATE utf8_bin NOT NULL,
  `PRIORITY` int(11) NOT NULL,
  PRIMARY KEY (`ID`),
  FULLTEXT KEY `FT` (`FT`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*Table structure for table `sk_substitution` */

DROP TABLE IF EXISTS `sk_substitution`;

CREATE TABLE `sk_substitution` (
  `REGEX` varchar(150) COLLATE utf8_bin NOT NULL,
  `REPLACEMENT` varchar(150) COLLATE utf8_bin NOT NULL,
  `PRIORITY` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`REGEX`),
  KEY `PRIORITY` (`PRIORITY`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Tabulka se seznamem regularnich vyrazu k nahrazeni v hledanem textu. K nahrazeni dochazi po odstraneni diakritiky z hledaneho vyrazu.';

/*Table structure for table `sk_zip` */

DROP TABLE IF EXISTS `sk_zip`;

CREATE TABLE `sk_zip` (
  `ID` int(11) NOT NULL,
  `ZIP` varchar(10) COLLATE utf8_bin NOT NULL,
  `FT` varchar(10) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`ID`),
  FULLTEXT KEY `FT` (`FT`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
