from mrjob.job import MRJob,MRStep

class ForbeniusCalculation(MRJob):
    """[summary]

    Args:
        MRJob ([type]): [description]
    """
    def steps(self):
        """ This method defines the number of steps involved to execute the mapreduce job."""
        return [
            MRStep(mapper_raw=self.mapper,
                   reducer=self.sum_reducer),
            MRStep(reducer = self.square_reducer)
        ]

    def mapper(self,_,file_path):
        """ The mapper function takes in the path of the A.txt file and yields the squared value of the matrix elements.

        Args:
            file_path (String): The path of the file
        Yields:
            float: sqaured matrix elements.
        """
        file_open = open(file_path,'r')
        for line in file_open.readlines():
            matrixValues = line.split()
            for index in range(len(matrixValues)):
                yield _,float(matrixValues[index])**2
    
    def sum_reducer(self,_,values):
        """ The sum_reducer function takes in the squared matrix values and yields the sums it.

        Args:
            values ([float]): The sqaured matrix elements.
        Yields:
            float: sum of the sqaured matrix elements.
        """
        yield "sum_of_all_squaredMatrixValues",sum(values)
                
    def square_reducer(self,_,value):
        """ The square reducer takes in the sum from the previous reducer and calculates the sqaure root. Which is the Frobenius Norm.

        Args:
            values (float): Sqaure sum of matrix elements 
        Yields:
            float: sqaure root of the sum_reducer value.
        """
        yield "frobenius_norm",(sum(value))**0.5
    
   
if __name__ == "__main__":
    ForbeniusCalculation().run()
