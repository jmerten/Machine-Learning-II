from mrjob.job import MRJob
from mrjob.step import MRStep
from io import open 

class AverageReviews(MRJob):

    #Configure self to add in the ancillary document u.item so movie ID can be 
    #replaced by movie names later
    def configure_args(self):
        super(AverageReviews, self).configure_args()
        self.add_file_arg('--items', help='Path to u.item')
    
    #Define steps 
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer = self.reducer_find_avgs)
        ]
    
    #Line.split the data input document. Use the mapper to yield the elements 
    #that are relevant to the assignment: MovieID and rating (in numeric)
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, float(rating)
    
    #Define a reducer_init()function to allow for a look up in the u.item so 
    #MovieID can be replaced with movie name
    def reducer_init(self):
        self.movieNames = {}

        with open("u.item", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]

    #This reducer uses a loop to lopp through values (i.e., ratings created by 
    #the previous mapper), to sum the ratings up for individual movies, count 
    #the occurrence of ratings for individual movies, and divide the former by 
    #the latter to get the average rating of the movie. Use view>=100 as a filter 
    #to keep only the movies that have greater than 100 ratings. 
    #The reducer yields movie names, average rating, and number of ratings. 
    def reducer_count_ratings(self, key, values):
        total = 0
        views = 0
        for x in values:
            total += x
            views += 1
        if (views >= 100):
            yield '%05f'%(total / views), (self.movieNames[key], views)

    #Passing through the reducer one more time so the average ratings can be sorted in an ascending order. 
    def reducer_find_avgs(self, values, key):
        yield values, key
        

if __name__ == '__main__':
    AverageReviews.run()

#ml-100k is in same folder as python file and working . u.item and u.data are the two documents needed for this task
#Execution Statement: copy and paste to the console to run
#!python Module2Group2.py --items=ml-100k/u.item ml-100k/u.data > SortedMoviebyRatings.txt