# Simple function to write out a progress tab of dots

import sys

def update():
     sys.stdout.write('.')
     sys.stdout.flush()
     return

def final():
     sys.stdout.write('\n')
     sys.stdout.flush()
     return