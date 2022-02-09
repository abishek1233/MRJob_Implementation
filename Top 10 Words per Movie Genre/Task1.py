import re,string
from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

class Top10Words(MRJob):
    """ The class Top10Words inherits from the MRJob library to perform the basic necessary functions.
    This class finds the top 10 most common keywords within the title for each possible movie genre in the movies.csv dataset.
    
    Args:
        MRJob : base class inherited from the library. Used for the mapreduce functionalities.
    """

    def steps(self):
        """ This method defines the number of steps involved to execute the mapreduce job."""
        return [MRStep(mapper=self.mapper,combiner=self.combiner,reducer = self.firstReducer),MRStep(reducer = self.secondReducer)]

    def mapper(self,_,lines):
        """ The mapper method takes the lines from the movies.csv file as the input and does all the necessary
        preprocessing of data and it gives us the key-value pair of the words in the title for each genres there is in the dataset.

        Args:
            lines (str): Each and every line of the movies.csv dataset.

        Yields:
            key-value pairs: key-value pair of the word and the genre and the occurrence of the word which is default 1  
        """
        result = lines.split(',')
        stop_words = set(stopwords.words('english'))
        genres = result[2].split('|')
        for genre in genres:
            for word in result[1].split(' '):
                if '(' not in word:
                    word = word.lower()
                    word = word.translate(str.maketrans('', '', string.punctuation))
                    word = ''.join(re.sub(r"[0-9]",'',word))
                    if word != '' and word not in stop_words:
                        yield (word,genre), 1


    def combiner(self,key,values):
        """Grouping the words and finding the sum of the total occurrences of the word for that genre.

        Args:
            key (str,str): the (word,genre) key-value pair
            values ([int]): the occurreces of the word(key) from the mapper which is 1

        Yields:
            key-value pairs: where key is the word and the values is the total count/sum of the occurrences of the word.
        """
        yield key,sum(values)

    def firstReducer(self,key,value):
        """ The same words count are added and their values are stored along with the word and the specific genre. 

        Args:
            key (str,str): The title word 
            value ([int]): The total count of the word.

        Yields:
            key-value pair: where key is the genre and the value is the tuple of the word and it's total occurrences.
        """
        word,genre = key
        yield genre,(word,sum(value))

    def secondReducer(self,key,values):
        """ This method is just used to sort the the key-value pairs according to their most occurrences and the top 10 of each genre is yielded.

        Args:
            key (str): The genre which is available.
            values ([str,int]): The word and it's corresponding total count.

        Yields:
            key-value pair: The top 10 most common words of each genre.
        """
        data = list(values)
        data.sort(key=lambda x: x[1],reverse=True)
        # Tried the default bubble sort method but since it took a toll on the time , did not use it.
        # lst = len(data)
        # for i in range(0, lst):  
        #     for j in range(0, lst-i-1): 
        #         if (data[j][1] < data[j + 1][1]): 
        #             temp = data[j] 
        #             data[j]= data[j + 1] 
        #             data[j + 1]= temp
        for value in data[:10]:
            word = value[0]
            if len(key.split(" ")) == 1:
                yield key,word

        

if __name__ == '__main__':
    Top10Words.run()
