# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

class Datastore(object):

    def __init__(self, dir):
        self.dir =dir
        self.schema = 


    def load(self, documents) -> Cluster:
        """
        LOAD documents INTO SQLITE DATABASES REPRESENTING COLUMNS
        :param documents:
        :return: cluster
        """
        # NORMALIZE
        # FOR EACH DOC
        # MAKE NEW DATABSE FOR UNKNOWN COLUMN

    def _merge(self, *clusters) -> Cluster:
        """
        MERGE A NUMBER OF CLUSTERS INTO ONE
        :param clusters: list of clusters
        :return: merged cluster
        """


    def query(self, query) -> Cluster:
        """
        SIMPLE AGGREGATE OVER SOME SUBSET, GROUPED BY SOME OTHER COLUMNS
        :param query:
        :return: cluster
        """


    def matmul(self, a, b) -> Cluster:
        """
        ASSUMING a AND b REFER TO MULTIDIMENSIONAL ARRAYS
        :return: MATRIX MULTIPLY IN A CLUSTER
        """

