import multiprocessing
from multiprocessing import Pool
import time
import csv
import datetime
import numpy as np
from os import listdir
from nltk.tokenize import sent_tokenize
import re
import spacy
nlp = spacy.load('en_core_web_sm')
import json
import pickle
import glob
import urllib
import urllib.parse
import urllib.request
from bs4 import BeautifulSoup
import string
import argparse

punc = set(string.punctuation) - {'.'}
punc = ''.join(punc)
num = '0123456789'
queried_dict = {}

class parallel_process():

	def __init__(self):
		self.results = []

	def open_file(self,filename):
		file = open(filename,encoding="utf-8")
		return file

	def chunks(self, l, n):
		for i in range(0, len(l), n):
			yield l[i:i + n]

	def remove_html_tags(self, text):
		"""Remove html tags from a string"""
		clean = re.compile('<.*?>')
		return re.sub(clean, ' ', text)

	def replace(self, string, char): 
		pattern = char + '{2,}'
		string = re.sub(pattern, char, string) 
		return string 

	def get_companiesList(self):
		with open('df_10k_1500_10_org.pkl', 'rb') as f:
			data = pickle.load(f)
			companies = list(np.unique(data['comp_name']))
			return companies 

	def extract_labels(self, query):
		api_key = "AIzaSyAoYrJhzoOmgZBNQmrprs1wbodi3E5pmMQ"
		service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
		headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
		label = 0


		if query not in queried_dict.keys():
			desc = ''
			params = {
				'query': query,
				'limit': 1,
				'indent': True,
				'key': api_key,
			}
			url = service_url + '?' + urllib.parse.urlencode(params)
			req = urllib.request.Request(url, headers=headers)
			response = json.loads(urllib.request.urlopen(req).read())
			for i,element in enumerate(response['itemListElement']):

				if('description' in element['result'].keys()):
					desc = str(element['result']['description'])

				if('Organization' in element['result']['@type']):
					label = 'org'
				elif('Place' in element['result']['@type']):
					label = 'Place'
				elif('Person' in element['result']['@type']):
					label = 'Person'
				elif('Thing' in element['result']['@type']):
					label = 'Thing'

			if(desc != ''):
				if('company' in desc.lower() or label == 'org'):  
					label='ORG'
			elif(label=='Person'):
				label='PER'
			elif(label=='Place'):
				label='LOC'
			elif(label == 'Thing'):
				label = 'PDT'
			else:
				label = 0
			queried_dict[query] = label
		else:
			return queried_dict[query]

		return label
	
	def clean(self, bs):
		final = ''
		for tag in bs.find_all():
			if (tag not in ['filestats', 'filename']) and (len(tag.text) > 500):
				text = tag.text.replace('\n', ' ')
				text = text.replace('\t', ' ')
				text = text.translate(str.maketrans('', '', punc))
				text = text.translate(str.maketrans('', '', num))
				text = re.sub('\s{2,}', ' ',text)
				
				final += text
		return final
	
	def extract_entities(self, sentences, filename):
		name = filename.split('/')[-1].split('.')[0]
		out_path = '/home/saiamrit/IRE_major_project/conll_latest/'
		f = open(out_path + name + '.conll' , 'w')

		for i, sentence in enumerate(sentences):
			doc = nlp(sentence)
			entity = ''
			iobs = ''
			ent_types = ''
			for x in doc:

				if(x.ent_iob_ == 'O'):
					if entity == '':
						f.write(str(x) + ' ' + '0 0\n')
					else:
						label = self.extract_labels(entity)
						for item, iob, ent_type in zip(entity.split(), iobs.split(), ent_types.split()): 
							f.write(str(item) + ' ' + str(iob) + '-' + str(ent_type) + ' ' + str(label) + '\n')
						entity = ''
						iobs = ''
						ent_types = ''
				elif x.ent_type_ in ['PERSON','ORG','GPE','LOC','PRODUCT']:
					entity += ' ' + str(x)
					iobs += ' ' + str(x.ent_iob_)
					ent_types += ' ' + str(x.ent_type_)
				else:
					f.write(str(x) + ' ' + '0 0\n')
			f.write('\n')

			
	def parse_chunk(self,filenames):
		results = []
		for i, filename in enumerate(filenames):
			try:
				print(filename)
				f = open(filename)
				soup = f.read()
				bs = BeautifulSoup(soup,"html.parser")
				clean_text = self.clean(bs)
				sentences = sent_tokenize(clean_text)
				entities = self.extract_entities(sentences, filename)
				# print(label_dict)
			except Exception as e:
				print(e)
		# print(num_entities)
		return results


	def run_process(self, input_path, output_path):
		# global num_entities
		path = '/home/saiamrit/IRE_major_project/10k_1500_10_org/'
		# companies = ['1520006', '1400810', '320335', '878927', '1021162']
		# companies = [320193, 72971]
		companies = ['1032220', '1545654', '71691', '1185348', '946673', '1590717', '1615817', '1385292', '52795', 
		'1360901', '704415', '1099800', '1336920', '39911', '1094285', '1175535', '6955', '1096752', '711404', 
		'821002', '84246', '316709', '896622', '851310', '278166', '1476045', '891024', '751364', '1094392', 
		'1022671', '939767', '1108524', '1000229', '108385', '730255', '887936', '1013871', '1001082', '831259', 
		'907242', '936395', '1650132', '1038205', '1206264', '743988', '26076', '1177648', '818479', '1095565', 
		'842023', '1362468', '1053532', '1097149', '63754', '354190', '1084961', '59478', '945764', '899689', '19617',
		 '40704', '1364250', '1339947', '77877', '40987', '46195', '1065280', '1390777', '79879', '1610950', '945394',
		  '39263', '78890', '1335258', '764180', '1571123', '72903', '48465', '1324272', '48898', '833640', '52988', 
		  '1005757', '14930', '1177702', '1044777', '18349', '106040', '1173489', '1434620', '1115055', '1571949', 
		  '23197', '933036', '922864', '1357615', '720005', '1289490', '1046025', '85535', '896262']





		# [ '94344', '8947', '1456772', '1585364', '1035267', '1459200', '853816', '1023313', 
		#  '1060822', '888491', '104918', '2034', '1451505', '57131', '36146', '350797', '1116132', '885245', '66570', 
		#  '1021860', '1051512', '822663', '1594686', '80661', '1133470', '1518715', '57725', '1493225', '314808', '1519751', 
		#  '1374756', '1598428', '1645494', '1274494', '765880', '1412100', '817720', '62996', '821127', '1443669', '918646', 
		#  '1318084', '1569187', '1472787', '1561680', '45012', '101829', '1281761', '1606498', '64803', '1283630', '3453', 
		#  '840489', '354950', '56873', '1593936', '1137774', '1408198', '1000623', '1525769', '1645590', '93389', '1013462', 
		#  '1636023', '854775', '1056903', '827871', '16918', '1587732', '1326801', '19584', '1613103', '1063344', '52827', 
		#  '1057706', '76267', '1611547', '1587523', '1125920', '1534504', '1067983', '1373715', '1635718', '792977', '1075415', 
		#  '1593034', '801337', '785161', '1069157', '105132', '1341439', '1475841', '1618921', '1464423', '1510295', '1283157', 
		#  '1689796', '54381', '1031316', '949158', '70858', '764622', '1441236', '9092', '819793', '1162461', '5272', '814184',
		#  '909832', '1091883', '1070985', '874716', '104889', '1174922', '1013237', '1070750', '1606363', '723254', '1037646',
		#  '1627223', '810136', '714310', '1393612', '1170010', '883569', '815097', '867374', '36047', '713676', '350698', 
		#  '1059556', '788784', '65270', '913144', '73756', '790051', '1384195', '701347', '1235010', '828916', '896156', 
		#  '922224', '877422', '21344', '884217', '108772', '95029', '30697', '1418819', '42888', '1298946', '784199', 
		#  '798949', '726854', '1275187', '1005409', '1336917', '91419', '1385157', '910108', '1024305', '1308161', 
		#  '1067294', '1442145', '67215', '101199', '769397', '1320414', '912728', '879585', '1437107', '850460', 
		#  '1633978', '936528', '23217', '1166691', '933974', '829224', '1005201', '1009672', '1029800', '860730', 
		#  '105634', '703351', '89439', '62234', '1037038', '1113169', '36966', '1069202', '1280452', '74260', 
		#  '890319', '311094', '1430723', '1128928', '1080014', '858655']




		# '1520006', '1400810', '320335', '878927', '1021162', '1528849', '1544229', '1437071', 
		# '68709', '1031203', '859737', '1000697', '895421', '101382', '1617898', '1547903', '36029', '1466258', 
		# '726728', '1013488', '799233', '318833', '66756', '1088825', '764478', '1047122', '1069878', '1111928',
		#  '906553', '62709', '1057877', '1620280', '320187', '105770', '1023024', '1175454', '785786', '1413329', 
		#  '718877', '1597033', '18498', '874501', '772406', '277948', '1049502', '763744', '880117', '1475922', 
		#  '743316', '1613859', '31791', '920424', '821026', '712537', '1022408', '1420302', '793733', '1364742', 
		#  '59440', '882095', '874866', '91440', '1601046', '903129', '1579241', '1021635', '1052100', '79282', 
		#  '73088', '907471', '1418091', '1039399', '923796', '726514', '14693', '107263', '827187', '96943', 
		#  '927066', '277509', '887905', '72331', '924901', '1050446', '864749', '1062231', '97476', '36270', 
		#  '891014', '1389050', '1620459', '793952', '887733', '701985', '1267238', '27996', '742112', '310158', 
		#  '917273', '1109357']
		 # '37996', '1555280', '1015780', '899629', '819220', '40211', '813672', '1002047', 
		 # '729986', '315709', '1525221', '712515', '1107843', '100493', '1552800', '1113232', '1113481', '1580670', 
		 # '2488', '56978', '1090727', '1357204', '1103982', '896159', '1500217', '103730', '1616318', '1031296', 
		 # '745732', '317540', '1559865', '76605', '833079', '1126956', '898174', '1283699', '866729', '729580', 
		 # '883984', '1530721', '1407623', '64040', '1356576', '1136893', '1585689', '1300514', '1144980', '87347', 
		 # '36104', '794172', '1601712', '795403', '896878', '1596783', '1604778', '313143', '949039', '910073', 
		 # '897077', '1707925', '1551182', '876427', '202058', '78003', '707549', '1053507', '1597672', '73124', 
		 # '795266', '1710366', '731802', '110621', '1071739', '1534992', '1042046', '1478242', '1669812', '1035002', 
		 # '1169561', '898293', '72207', '12208', '66382', '21665', '705432', '832101', '10456', '907254', '1381197', 
		 # '70145', '6769', '1278021', '780571', '1581091', '1201792', '921738', '22444', '63908', '716643', '926326', 
		 # '1410636', '1070412', 
		 # '94344', '8947', '1456772', '1585364', '1035267', '1459200', '853816', '1023313', 
		 # '1060822', '888491', '104918', '2034', '1451505', '57131', '36146', '350797', '1116132', '885245', '66570', 
		 # '1021860', '1051512', '822663', '1594686', '80661', '1133470', '1518715', '57725', '1493225', '314808', '1519751', 
		 # '1374756', '1598428', '1645494', '1274494', '765880', '1412100', '817720', '62996', '821127', '1443669', '918646', 
		 # '1318084', '1569187', '1472787', '1561680', '45012', '101829', '1281761', '1606498', '64803', '1283630', '3453', 
		 # '840489', '354950', '56873', '1593936', '1137774', '1408198', '1000623', '1525769', '1645590', '93389', '1013462', 
		 # '1636023', '854775', '1056903', '827871', '16918', '1587732', '1326801', '19584', '1613103', '1063344', '52827', 
		 # '1057706', '76267', '1611547', '1587523', '1125920', '1534504', '1067983', '1373715', '1635718', '792977', '1075415', 
		 # '1593034', '801337', '785161', '1069157', '105132', '1341439', '1475841', '1618921', '1464423', '1510295', '1283157', 
		 # '1689796', '54381', 

		# '64996', '32604', '1609702', '816761', '858877', '1513761', '1158114', '753308', '1666700', '1424929',
		#   '1521036', '746515', '72741', '1681459', '1467858', '76334', '1158863', '1506307', '1408710', '700923', '1639300', 
		#   '6951', '1622194', '1673985', '1514991', '1486159', '1579684', '880631', '1526113', '1724670', '1692115', '1659166', 
		#   '1660690', '860748', '1576018', '1670541', '1495320', '1613665', '1419612', '1424755', '1690666', '1596532', '1634117', 
		#   '759944', '1599489', '1681622', '1610092', '1671013', '1599617', '1653477', '1428336', '1701605', '1524472', '1679273', 
		#   '1730168', '1669811', '1688568', '1633931', '1674862', '1624899', '1683606', '1632127', '1707092', '1680247', '1274173']

		listify = []
		yrs = ['2007' , '2012' , '2018']
		for c in companies:
			for y in yrs:
				ly = sorted(glob.glob(path + '{}_{}*_10-K_*'.format(c,y)))
				listify.extend(ly)

		print(len(companies))
		print(len(listify))
		
		data = self.chunks(listify, int(len(listify) / (multiprocessing.cpu_count() - 2)))
		p = Pool(processes=multiprocessing.cpu_count() - 2)
		results = [p.apply_async(self.parse_chunk, args=(list(x),)) for x in data]
		# wait for results
		results = [item.get() for item in results]
		

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("--input_path", required=True,
						help="Documents path")
	parser.add_argument("--output_path", required=True,
						help="Path to save output CONLL files")

	args = parser.parse_args()

	c = parallel_process()
	c.run_process(args.input_path, args.output_path)