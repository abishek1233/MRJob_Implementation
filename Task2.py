from mrjob.job import MRJob,MRStep

class reverseWebGraph(MRJob):
    """ The class reverseWebGraph inherits from the MRJob library to perform the basic necessary functions.
    This class is used to reverse the web-google link graph and output a list of all the possible source nodes for a given target node.
    
    Args:
        MRJob : base class inherited from the library. Used for the mapreduce functionalities.
    """

    def steps(self):
        """ This method defines the number of steps involved to execute the mapreduce job."""
        return [MRStep(mapper=self.mapper,reducer = self.reducer)]
    
    def mapper(self,_,line):
        """ The mapper function takes the lines from the web-google.txt file and yields the reversed web-link graphs. 
        It yields the destinationNode and the sourceNode.

        Args:
            lines (str): The input lines are the data lines which contains the source and destination node data from the web-Google.txt file.
        """
        # to skip the first few lines and read only from the line which contains the data.
        if '#' not in line:
            fromNode,toNode = line.split()
            yield toNode,fromNode
    
    def reducer(self, toNode, fromNode):
        """ The reducer iterates through all the possible source nodes for a given target node and yields us a list of the same.

        Args:
            toNode (str): Target Node 
            fromNode ([str]): a list of the Source Nodes.

        Yields:
            key-value pair: which is the target node and the list of all possible source nodes for it.
        """
        nodes = [node for node in fromNode]
        yield toNode,",".join(nodes)

if __name__ == '__main__':
    reverseWebGraph.run()