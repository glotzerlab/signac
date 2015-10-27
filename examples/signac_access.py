import os
import re

import signac
from signac.contrib.formats import TextFile


# Define a crawler class for each structure
MyCrawler(RegexFileCrawler): pass

# Add file definitions for each file type, that should be part of the index.
MyCrawler.define(re.compile('a_(?P<a>\d+\.txt'), TextFile)

# Expose the data structures to a master crawler
def get_crawlers(root):
  return {
    # the crawler name is arbitrary, but is required for identification
    'main': MyCrawler(os.path.join(root, 'my_project'))
    }
