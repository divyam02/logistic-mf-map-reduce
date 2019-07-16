from mrjob.job import MRJob

class processing(MRJob):
	def mapper(self, key, record): # Yields a key.
		print("in mapper...")
		print("key", key)
		print("record", record)
		yield [int(record)+4, 1] # replace with step counter??

	def reducer(self, key, records):
		print("in reducer...")
		print(key, records)
		yield key, list(records) # generator object contains all key associated value, in a list...
								 # let's try looping this.

	def mapper_words(self, key, line):
		for word in line.split():
			yield word, 1

	def reducer_words(self, word, occurences):
		yield word, sum(occurences)

if __name__ == '__main__':
	processing.run()
