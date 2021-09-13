# Discussion Code
# Name the file "arec-f.py"
# Execution Statement
# !python Module3Group2.py ainput-f.txt > afriends.txt
# Note, this took about 10 minutes to run on Dr. Wilck's work CPU


#Import MRJob and MRStep packages
from mrjob.job import MRJob
from mrjob.step import MRStep

#Put all the MapReduce information into a MRJob class.
class FriRec(MRJob):

#Define the two steps of the program. Each step has a mapper and a reducer. The mapper
#and reducer in the second step will overwrite the mapper and reducer in the first step. 
    def steps(self):
        return  [
            MRStep(mapper = self.mapper1,
                   reducer = self.reducer1),
            MRStep(mapper = self.mapper2, 
                   reducer = self.reducer2)
            ]

#Line.split the input data which is a .txt document. The .txt document has two columns - first column
#has User_ID and the second column include the IDs of all the connected friends of the customer. The columns
#are divided by tabs, so we use ("/t") to unpack.
#Then we go a level deeper, we assign the second column to "friends", and then use (",") to split up each item
#in "friends" to grab the all the friend_ids in the connections.
    def mapper1(self, _, line):
        user_id, friends = line.split("\t")
        friend_ids = friends.split(",")

#Now we pair the user_id with each of the connected friends, and we assign a value of 0 to each pair so that we 
#distinguish these pairs from another set pairs that will created later. For example, row 1 will yield (0,1,0),(0,2,0),(0,3,0)...
        for friend in friend_ids:
            yield (user_id + "," + friend, 0)
#The second sets of pairs are pairing up all the friend IDs in a user's connections.
#Later, we will identify if these pairs are truly connected in the social network, and if not, 
#we will determine if we will recommend the connection to the pairs. In this pairing process, 
#since we don't want to pair, for instance in Row 1, Friend_ID 1 with Friend_ID 1, so 
#whenever a friend_id=friend_id, we will skip it in the loop, and only keep the pairs where the friend IDs
#are different. A person does not need to befriend herself/himself. For these pairs, we assign a 
#value of 1 to distinguish them from the pairs created in lines 36-37.
#For example, row 1 will yield (1,2,1),(1,3,1),(1,4,1).... 
#So for mapper1, key is: pair of user IDs, value is: 0 or 1
        for friend_i in friend_ids:
            for friend_j in friend_ids:
                if friend_i == friend_j:
                    continue
                yield (friend_i + "," + friend_j, 1)

#Now we send all the pairs created in lines 36-37 and lines 46-50 to a reducer. The key is a string of a user ID, 
#a comma, and another user ID. The value is number - either 0 or 1. So we will have the scenarios of coexistence of 
#(user ID 1, user ID 2, 0) and (user ID 1, user ID 2, 1), with the former mean the two users already have befriended 
#each other, and the latter mean the two users have mutual friends. Here, we only want to capture the scenarios where 
#two users have mutual connections but haven't befriended each other yet, so we use the "if not (0 in values)" as the
#filter. The reducer will only add up all the applicable 1s. The larger the sum, the higher frequency of the mutual 
#connections shared by the two users. For instance, if one of the yields is (2022, 3257, 20), that would mean 20 users 
#have befriended both User 2022 and User 3257 in their network. In other words, User 2022 and User 3257 have 20 mutual
#friends but haven't befriended each other yet.
#So for reducer1, key is: pairs of user ID who haven't befriended each other, value is: counts of mutual connections between the pair.
    def reducer1(self, friend_pair, values):
        if not (0 in values):
            yield (friend_pair, sum(values))

#Now we use a second mapper to rearrange the key and value to get ready for sorting (by number of mutual connections). 
#We split up value which is in the form of string of user ID, comma, and user ID. We make the first user ID the key, 
#and combine the second user ID and the sum as a string and make it the value. So the previous example of 
#((2022,3257), 20) is remapped to (2022, (3257,20)).  
#So for mapper2, key is: user ID, value is: user ID (recommended friend) and sum of mutual connections    
    def mapper2(self, friend_pair, value_sum):
        user_id, friend_id = friend_pair.split(",")
        yield (user_id, friend_id + "," + str(value_sum))

#Use the second reducer to generate output of a comma-separated list of unique user IDs with recommendation of
#friend IDs. The order of the friend IDs are based on number of mutual connections - in a decreasing order. 
    def reducer2(self, user_id, values):
        #Use the value from the mapper2 to create a all_friends list with new tuples. And the new tuples flip 
        #the previous value example of (3257, 20) to (20,3257). 
        #The all_friends list may look like [(20,3257),(10,36875),(50,6743)...]. And we are ready to sort the list
        #by the counts of mutual friends, e.g., 20, 10, 50...
        all_friends = []
        for value in values:
            friend_id, value_sum = value.split(",")
            friend_id = int(friend_id)
            value_sum = int(value_sum)
            all_friends.append((value_sum, friend_id))
            
        #We sort the items in each of the all_friends list in a decreasing order. If a list has more than 
        #10 tuples, i.e., more than ten recommended friends, then we only keep the first 10 tuples. These 
        #10 tuples represent the 10 friends with whom the user has the highest number of mutual friends in
        #the network. We create a new list of tuples for this - most_common_friends. The list may look like 
        #[(200,5674),(195,7464),(150,7847)...], with a length of 10. So, counts of mutual friends and friend ID.
        most_common_friends = sorted(all_friends, reverse = True)
        if len(most_common_friends) > 10:
            most_common_friends = most_common_friends[:10]
    
        #For the tuples in the list of most_common_friend, we only want to keep the friend ID and we no longer need 
        #the sum of counts, because we are done with sorting. Now the friend IDs will become the value in the new tuples 
        #yielded by the reducer#2. And the key for the final tuples will be the user ID (generated in line 31). If we use 
        #the example in line 60, the final tuple of user #2022 would be (2022, Friend ID 1, Friend ID 2, ...., Friend ID 10). 
        #Friend ID 1 are most likely to show up with User 2022 in others' network, and in other words, User 2022 has the most
        #mutual friends with Friend ID 1.
        #So for reducer2, key is: unique user ID, value is: IDs of ten recommended friends.
        friend_id_strs = []
        for fri in most_common_friends:
            friend_id_strs.append(str(fri[1]))
        yield (user_id, ",".join(friend_id_strs))

if __name__ == '__main__':
    FriRec.run()
    
