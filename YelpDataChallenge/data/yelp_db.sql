CREATE DATABASE  IF NOT EXISTS `yelp` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `yelp`;
-- MySQL dump 10.13  Distrib 5.7.12, for osx10.9 (x86_64)
--
-- Host: localhost    Database: yelp
-- ------------------------------------------------------
-- Server version	5.7.15

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `checkin`
--

DROP TABLE IF EXISTS `checkin`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `checkin` (
  `business_id` varchar(45) CHARACTER SET utf8 NOT NULL,
  `day_of_week` int(11) NOT NULL COMMENT '0: Sunday\n1: Monday\n2: Tuesday\n3: Wednesday\n4: Thursday\n5: Friday\n6: Saturday',
  `hour_0` int(11) DEFAULT '0',
  `hour_1` int(11) DEFAULT '0',
  `hour_2` int(11) DEFAULT '0',
  `hour_3` int(11) DEFAULT '0',
  `hour_4` int(11) DEFAULT '0',
  `hour_5` int(11) DEFAULT '0',
  `hour_6` int(11) DEFAULT '0',
  `hour_7` int(11) DEFAULT '0',
  `hour_8` int(11) DEFAULT '0',
  `hour_9` int(11) DEFAULT '0',
  `hour_10` int(11) DEFAULT '0',
  `hour_11` int(11) DEFAULT '0',
  `hour_12` int(11) DEFAULT '0',
  `hour_13` int(11) DEFAULT '0',
  `hour_14` int(11) DEFAULT '0',
  `hour_15` int(11) DEFAULT '0',
  `hour_16` int(11) DEFAULT '0',
  `hour_17` int(11) DEFAULT '0',
  `hour_18` int(11) DEFAULT '0',
  `hour_19` int(11) DEFAULT '0',
  `hour_20` int(11) DEFAULT '0',
  `hour_21` int(11) DEFAULT '0',
  `hour_22` int(11) DEFAULT '0',
  `hour_23` int(11) DEFAULT '0',
  PRIMARY KEY (`business_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `business`
--

DROP TABLE IF EXISTS `business`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `business` (
  `business_id` varchar(45) NOT NULL,
  `name` varchar(100) NOT NULL,
  `full_address` varchar(100) NOT NULL,
  `city` varchar(45) NOT NULL,
  `state` varchar(5) NOT NULL,
  `longitude` float NOT NULL,
  `latitude` float NOT NULL,
  `stars` float DEFAULT '0',
  `review_count` int(11) DEFAULT '0',
  `open` tinyint(4) DEFAULT '0',
  PRIMARY KEY (`business_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `review`
--

DROP TABLE IF EXISTS `review`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `review` (
  `business_id` varchar(45) CHARACTER SET utf8 NOT NULL,
  `user_id` varchar(45) CHARACTER SET utf8 NOT NULL,
  `stars` decimal(2,1) NOT NULL,
  `text` text,
  `date` date NOT NULL,
  `votes_funny` int(11) DEFAULT '0',
  `votes_useful` int(11) DEFAULT '0',
  `votes_cool` int(11) DEFAULT '0',
  PRIMARY KEY (`business_id`,`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tip`
--

DROP TABLE IF EXISTS `tip`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tip` (
  `text` text NOT NULL,
  `business_id` varchar(45) CHARACTER SET utf8 NOT NULL,
  `user_id` varchar(45) CHARACTER SET utf8 NOT NULL,
  `date` date NOT NULL,
  `likes` int(11) DEFAULT '0',
  PRIMARY KEY (`business_id`,`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user`
--

DROP TABLE IF EXISTS `user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user` (
  `user_id` varchar(45) CHARACTER SET utf8 NOT NULL,
  `name` varchar(45) CHARACTER SET utf8 NOT NULL,
  `review_count` int(11) NOT NULL,
  `average_stars` decimal(3,2) NOT NULL,
  `yelping_since` date NOT NULL,
  `fans` int(11) DEFAULT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_compliment`
--

DROP TABLE IF EXISTS `user_compliment`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_compliment` (
  `user_id` varchar(45) NOT NULL,
  `profile` int(11) NOT NULL,
  `cute` int(11) NOT NULL,
  `funny` int(11) NOT NULL,
  `plain` int(11) NOT NULL,
  `writer` int(11) NOT NULL,
  `list` int(11) NOT NULL,
  `note` int(11) NOT NULL,
  `photos` int(11) NOT NULL,
  `hot` int(11) NOT NULL,
  `cool` int(11) NOT NULL,
  `more` int(11) NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_elite_status`
--

DROP TABLE IF EXISTS `user_elite_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_elite_status` (
  `user_id` varchar(45) NOT NULL,
  `year` int(11) NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_friend`
--

DROP TABLE IF EXISTS `user_friend`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_friend` (
  `user_id` varchar(45) NOT NULL,
  `friend_user_id` varchar(45) NOT NULL,
  PRIMARY KEY (`user_id`,`friend_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `user_vote`
--

DROP TABLE IF EXISTS `user_vote`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_vote` (
  `user_id` varchar(45) NOT NULL,
  `funny` int(11) DEFAULT '0',
  `useful` int(11) DEFAULT '0',
  `cool` int(11) DEFAULT '0',
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-10-12 18:07:51
