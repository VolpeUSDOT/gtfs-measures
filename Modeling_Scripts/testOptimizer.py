## This works for simple linear regression of a single value
## ----------------
#from scipy import stats
#
#import numpy as np
#x = np.random.random(10)
#y = np.random.random(10)
#
#slope, intercept, r_value, pv_value, std_err = stats.linregress(x,y)
#
#print 'r-squared:', r_value**2
#
#print x
#print y
#print slope, intercept
#
## -----------------



# This works for linear combination (and we can manually manipulate individual
# elements to create polynomial or exponential components in a limited way)
import pandas
import statsmodels.formula.api as sm
 
testval_y = [32, 16, 3]
testval_x1 = [3, 2, -1]
textval_x2 = [3, 1, 4]

df = pandas.DataFrame({"y": testval_y,"x1": testval_x1, "x2": textval_x2})
result = sm.ols(formula="y ~ x1 + x2",data=df).fit()
print result.params
print result.summary()