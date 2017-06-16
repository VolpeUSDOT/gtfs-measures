# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt


def plot_comparison(real, estimated, ax_max, route_type_string, adj_rsquared):
     plt.figure()
     plt.subplot(111)
     plt.scatter(real,estimated,s=10,c='b')
     plt.plot([0,ax_max],[0,ax_max],c='r')
     plt.ylim([0,ax_max])
     plt.xlim([0,ax_max])
     plt.xlabel('Real Ridership')
     plt.ylabel('Estimated Ridership')
     plt.text(ax_max*0.1,ax_max*0.9,'Route Types: %s' % route_type_string)
     plt.text(ax_max*0.1,ax_max*0.7,'Adj. R-Squared: %.3f' % adj_rsquared)
     plt.show()
     return