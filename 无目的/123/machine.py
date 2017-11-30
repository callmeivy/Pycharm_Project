#!/usr/bin/env python
import os
import feedparser
ny = feedparser.parse('http://www.zaobao.com/')
print ny
print type(ny)