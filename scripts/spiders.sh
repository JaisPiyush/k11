#!/bin/bash

# Moved up on the k11 folder
echo `pwd`
source ../../bin/activate
cd ..
# Starting docker container
docker-compose  up -d
docker-compose up -d
cd k11/digger
echo `pwd`
../../../bin/python3 -m scrapy crawl rss_feed_spider
sleep 2
../../../bin/python3 -m scrapy crawl html_feed_spider
sleep 5
../../../bin/python3 -m scrapy crawl html_article_spider
cd ..
docker-compose stop







