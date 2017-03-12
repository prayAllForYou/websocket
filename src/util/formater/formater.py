#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re

reload(sys)
sys.setdefaultencoding("utf-8")

class Formater:

#    def number_format(self, num, places = 0):
#        """Format a number with grouped thousands and given decimal places"""
#        #is_negative = (num < 0)
#        #if is_negative:
#        # num = -num
#
#        places = max(0, places)
#        tmp = "%.*f" % (places, num)
#        point = tmp.find(".")
#        integer = (point == -1) and tmp or tmp[:point]
#        decimal = (point != -1) and tmp[point:] or ""
#
#        count = commas = 0
#        formatted = []
#        for i in range(len(integer) - 1, 0, -1):
#            count += 1
#            formatted.append(integer[i])
#            if count % 3 == 0:
#                formatted.append(",")
#        formatted.append(integer[0])
#        integer = "".join(formatted[::-1])
#        return integer+decimal

    def number_format(self, num):
        orig = str(num)
        new = re.sub("^(-?\d+)(\d{3})", '\g<1>,\g<2>', orig)
        if orig == new:
            return new
        else:
            return self.number_format(new)



if __name__ == "__main__":
    print Formater().number_format(123100000000000000000)
