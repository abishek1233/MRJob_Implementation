from mrjob.job import MRJob
from preProcess import fetchLabels,eucledian_dist
import sys
import operator

class KnnMapRed(MRJob):
    """ This class performs KNN classification for the given Dataset.
    """
    main_file = sys.argv[-1]
  
    def featureCountDict(self, value):
        """ This function creates a dictionary and keeps labels as key and the Number of occurrences as its values.

        Args:
            value (list): Train data

        Returns:
            [Dict]: Key-Value pair of Train Data labels and its total occurrences.
        """
        dict_m = {}
        for x in value :
            classes = x[1]
            if x[1] in dict_m:
                dict_m[classes]+=1
            else:
                dict_m[classes] = 1
        return dict_m

    def mapper(self, key, line):
        """ The mapper function takes the lines from the normalizedIris.csv file and yields the ID and the Eucledian Distance.
        Args:
            line (str): Line from normalizedIris.csv

        Yields:
            key-value pair: the ID and the Eucledian Distance.
        """
        value = line.split(',')
        if value[0] != 'Id':
            label = value[-1]
            alist = list(map(float,value[0:-1]))
            trainData = fetchLabels()
            if label == '':
                for i in trainData.values.tolist():
                    yield alist[0],(eucledian_dist(i[1:-1],alist[1:-1]),i[-1])
    
    def reducer(self, key, values):
        """ This function Classifies and gives the output.

        Args:
            key (float): The ID of the corresponding Labels
        Yields:
            key-value pair: Classification Result.
        """
        data = list(values)
        data = sorted(data,key=lambda x: x[0])
        countDict = self.featureCountDict(data)
        yield key,max(countDict.items(), key=operator.itemgetter(1))[0]
          
if __name__ == '__main__':
    KnnMapRed.run()