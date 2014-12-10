
import os
import sys
from collections import deque

class Flog_Merger(object):
	_merge_started = False
	_avro_extention = '.avro'
	_avro_stats_record = None


	def __init__(self, path, new_filename):

		try:
			avro_files = filter(lambda x: x.endswith(self._avro_extention), iter(os.listdir(path))
			schema = avro.schema.parse(open(schema_file).read())
			self._writter = DataFileWritter(output_file, 'w'), DatumWriter(), schema, 'deflate')
		except Exception as e
			raise avro.schema.AvroException(e)
			sys.exit(1)


	def flog_metadata_handler(func):
		""" This is a decorator that handles avro meta data as well as very last stats record 
			in each file during merging
		"""	
		def wrapper(self, avro_records):
			""" Wrapper method for consuming flog avro file
			"""
			# Handle meta data
			if self._writter.tell() != 0: # TODO, need to fix this
				next(avro_records)

			# Handle stats line
			self._avro_stats_record = deque(avro_records, maxlen=1).pop()

			func(avro_records)

		return wrapper
		

	@flog_metadata_handler
	def consume_avro(self, avro_records):
		""" Write the avro data from the butter to file
		"""
		map(self._writter.append, iter(self._avro_record))

	
	def merge(self):
		""" Loop through the avros and merge each file
		"""
		for target_file in os.listdir(input_dir):
		    if target_file.endswith(".avro"):
		    	try:
			 	 	avro_records = DataFileReader(open(input_dir+"/"+target_file, "r"), DatumReader())
		 	 	except Exception as e:
		 	 		raise avro.schema.AvroException(e)

		 	 # Consume the records!
		 	 self.consume_avro(avro_records)

		# Write stats data to the last of the file
		self._writter.append(self._avro_stats_record)
		self._writter.close()


""" This is test code
"""
if __name__ == '__main__':
	path = None
	new_filename = None

	fm = FlogMerger(path, new_filename)
	fm.merge()

