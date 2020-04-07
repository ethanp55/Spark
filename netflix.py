import sys
from operator import add, concat

from pyspark import SparkContext

def movieMap(line):
    user_movie_review = line.split('\t')

    return ((user_movie_review[1], user_movie_review[2]), [user_movie_review[0]])

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: netflix <input> <user_id> <very_similar_users_output>")
        exit(-1)

    sc = SparkContext.getOrCreate()

    netflix_data = sc.textFile(sys.argv[1])

    user = sys.argv[2]

    others_with_same_reviews = netflix_data.map(movieMap) \
                                           .reduceByKey(concat) \
                                           .filter(lambda pair: user in pair[1]) \
                                           .flatMap(lambda pair: [(person, pair[0]) for person in pair[1] if person != user])

#     similar_users = others_with_same_reviews.collect()


#     for key, value in similar_users:
#         print("%s: %s" % (key, value))

#     print('\n\n\n------------------------\n\n\n')

    others_with_most_similar_reviews = others_with_same_reviews.map(lambda pair: (pair[0], 1)) \
                                                               .filter(lambda pair: user != pair[0]) \
                                                               .reduceByKey(add) \
                                                               .sortBy(lambda pair: pair[1], ascending = False)

#     top_pair = others_with_most_similar_reviews.take(1)
#     most_similar_reviews = top_pair[0][1]

#     very_similar_users = others_with_most_similar_reviews.filter(lambda pair: pair[1] == most_similar_reviews) \
#                                                          .collect()

    very_similar_users = others_with_most_similar_reviews.take(10)

    for key, value in very_similar_users:
        print("%s: %s" % (key, value))

    others_with_most_similar_reviews.saveAsTextFile(sys.argv[3])

    sc.stop()
