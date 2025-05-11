"""
 ###  #   #  #  #   ##   #  #   ##   ###   ###         #      ##    ###  #  #
 #  #  # #   ## #  #  #  ####  #  #  #  #  #  #        #     #  #  #     # #
 #  #   #    # ##  #  #  ####  #  #  #  #  ###         #     #  #  #     ##
 #  #   #    #  #  ####  #  #  #  #  #  #  #  #        #     #  #  #     # #
 ###    #    #  #  #  #  #  #   ##   ###   ###         ####   ##    ###  #  #

       ####   ##   ###         #      ##   #  #  ###   ###    ##    ###
       #     #  #  #  #        #     #  #  ####  #  #  #  #  #  #  #
       ###   #  #  ###         #     #  #  ####  ###   #  #  #  #   ##
       #     #  #  # #         #     ####  #  #  #  #  #  #  ####     #
       #      ##   #  #        ####  #  #  #  #  ###   ###   #  #  ###

DynamoDBLock is a distributed locking mechanism using DynamoDB. It's designed 
for scenarios where multiple concurrent Lambda executions need to ensure that 
certain tasks are performed exclusively by a single lambda execution.

Author.: Ricardo Abuchaim - ricardoabuchaim@gmail.com
Github.: http://github.com/rabuchaim/dynamodblock
Issues.: https://github.com/rabuchaim/dynamodblock/issues
PyPI...: https://pypi.org/project/dynamodblock/  ( pip install dynamodblock )
Version: 1.0.2 - Release Date: 11/May/2025
License: MIT

"""
from dynamodblock.dynamodblock import *