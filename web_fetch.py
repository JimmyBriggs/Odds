#
#   Script for fetching useful data from websites
#

import os
#import pyodbc
import socket
import urllib
import urllib2
import json
#import multiprocessing
#import Queue
import string
import time
import re
#import EasyDialogs
#import pyPdf

#
#   Globals
#
default_dest=r'D:\james\course_files'
default_tga_url=r'http://training.gov.au/TrainingComponentFiles/'
industry_map={}
industryspecific_map={}

# example from website: http://docs.python.org/howto/urllib2.html
TIMEOUT = 50
socket.setdefaulttimeout(TIMEOUT)

industry_page='http://joboutlook.gov.au/pages/industry.aspx'
industry_specific_page='http://joboutlook.gov.au/pages/industryspecific.aspx?search=industry&industry='
job_outlook_prefix='http://joboutlook.gov.au/Pages/occupation.aspx'
job_overview_page='http://joboutlook.gov.au/Pages/occupation.aspx?search=alpha&ampcode=%4d'
job_prospects_page='http://joboutlook.gov.au/Pages/occupation.aspx?code=%4d&ampsearch=alpha&ampTab=prospects'
job_earnings_page='http://joboutlook.gov.au/Pages/occupation.aspx?code=%4d&ampsearch=alpha&ampTab=stats&ampgraph=EA'
industry_list_start_str='<ul><li><a href="industryspecific.aspx?search=industry'
specific_jobs_start_str='<ul><li><a href="occupation.aspx'
list_end_str='</a></li></ul>'

search_strings={'industry':(';industry=','">','</a></li>'),
                'specific':(';code=','">','</a></li>')}

industry_map={}
industryspecific_map={}

occupation_map={}
field_map={}

job_prospects_summary_map={}
job_earnings_summary_map={}

#
# Google map stuff
google_map_api='maps.googleapis.com/maps/api/geocode'
google_map_data_type='json'

#
# ABS occupation stuff
abs_occupation_prefix='http://www.abs.gov.au/AUSSTATS/abs@.nsf/Latestproducts/'
abs_occupation_contents_table='1220.0Contents0First%20Edition,%20Revision%201?opendocument&tabname=Summary&prodno=1220.0&issue=First%20Edition,%20Revision%201&num=&view='
abs_level_map = { 1:'Major Group', 2:'Sub-Major Group', 3:'Minor Group', 4:'Unit Group' }
abs_subpages_tags=['a']
anzsco_descriptions={}
anzsco_titles={}

#
# ABS field stuff
abs_fields_prefix='http://www.abs.gov.au/ausstats/abs@.nsf/Latestproducts/'
abs_fields_definition_table='6E04E37B83201BCFCA256AAF001FCA5D?opendocument'
abs_field_level_map= { 1:'Broad Field', 2:'Narrow Field', 3:'Detailed Field' }
abs_subpages_tags=['a']
field_descriptions={}
field_titles={}

#
# Training.gov.au PDF extraction stuff
description_title='DESCRIPTION'
entry_requirements_title='ENTRY REQUIREMENTS'

#
# Microsoft Access constants
access_connect_str='DRIVER={Microsoft Access Driver (*.mdb)};DBQ=%s'
default_source_file=r'D:\james\data\TAFEDIR.mdb'
default_location=os.path.dirname(default_source_file)
default_db_typelist=['mdb','accdb']
default_target_file=r'D:\james\data\skills_navi_staging.mdb'

#
# Regular expressions
#
tags=('acronym',)
all_tag_pairs={}
for tag in tags:
    a=re.compile('<'+tag)
    b=re.compile('</'+tag+'>')
    all_tag_pairs[tag] = (a,b)

# regex expression for matching ABS chapitems
chapitem_regex=re.compile('id="chapitem">')
# A filter function
def chapitem_regex_filter(s):
    return chapitem_regex.search(s)


# regex expression for matching start title
start_title_regex=re.compile('<!-- Start Title -->')
occupations_regex=re.compile('Occupation:|Occupations:')
alternative_regex=re.compile('Alternative Title:|Alternative Titles:')
skill_level_regex=re.compile('Skill Level:')
specialisation_regex=re.compile('Specialisation:|Specialisations:')
nec_occupations_regex=re.compile('Occupations in this group include:')
anzsco_code_regex=re.compile('[1-9][1-9][1-9][1-9][1-9][1-9]')

parsing_occupation_skip_list=[alternative_regex,skill_level_regex,specialisation_regex]

detailed_field_list_regex=re.compile('This narrow field comprises the following detailed field')
examples_field_regex=re.compile('Examples of subjects in this detailed field include:')
end_of_descriptions_regex=re.compile('<!-- Start Banner Advertisement -->')
exclusions_regex=re.compile('Exclusions:',re.IGNORECASE)
broad_field_code_regex=re.compile('[0-1][0-9] ')
narrow_field_code_regex=re.compile('[0-1][0-9][0-9][0-9]')
detailed_field_code_regex=re.compile('[0-1][0-9][0-9][0-9][0-9][0-9]')

overview_start_html_regex=re.compile('<h2>Overview</h2>')
overview_end_html_regex=re.compile('<h2>Tasks</h2>')
prospects_start_html_regex=re.compile('<h2>Job Prospects</h2>')
prospects_end_html_regex=re.compile('<h2>Key Indicators</h2>')
statistics_start_html_regex=re.compile('<h2>Statistics</h2>')
statistics_end_html_regex=re.compile('<a href="#top">Back to Top</a>')
bad_description_regex=re.compile('excluded from this minor group|excluded from this unit group')

parsing_prospects_filters={ 'overview':(overview_start_html_regex,overview_end_html_regex),
                            'prospects':(prospects_start_html_regex,prospects_end_html_regex),
                            'statistics':(statistics_start_html_regex,statistics_end_html_regex) }

#
#   SQL
#

delete_industry_occupations_sql="""\
DELETE FROM Industry_occupations
"""

insert_industry_occupations_sql="""\
INSERT INTO Industry_occupations ( Industry_code, description, job_code, job_description )
VALUES (?,?,?,?)
"""

select_delivery_locations_sql="""\
SELECT campus_id, asqa_code, site_name, add1, add2, city, postcode, state
FROM Delivery_locations
WHERE (Delivery_locations.latitude Is Null OR Delivery_locations.longitude Is Null or
       Delivery_locations.latitude = 0 OR Delivery_locations.longitude = 0)
  and postcode is not null
"""

update_delivery_locations_sql="""\
UPDATE Delivery_locations
SET latitude = ?, longitude = ?
WHERE campus_id = ?
"""

update_field_description_sql="""\
UPDATE Field_of_education
SET description = ?
WHERE foe_id = ?
"""

update_occupation_description_sql="""\
UPDATE Occupations
SET description = ?
WHERE anzsco = ?
  and job_name_type in ('P','G')
"""

update_occupation_short_description_sql="""\
UPDATE Occupations
SET description = ?
WHERE anzsco = ?
  and job_name_type <> 'P'
"""

select_job_codes_sql="""\
SELECT anzsco, job_name, description
     , Count(Course_occupations.course_id)
FROM Occupations
     INNER JOIN Course_occupations
     ON Occupations.anzsco = Left(Course_occupations.anzsco,4)+'00'
WHERE anzsco like '%s' and anzsco not like '%s'
  AND Course_occupations.weighting > 3
GROUP BY anzsco, job_name, description
HAVING Count(Course_occupations.course_id)>1
ORDER BY anzsco
"""

select_geographic_location_sql="""
SELECT geographic_id, location, postcode, latitude, longitude
FROM Geographic_locations
WHERE (latitude Is Null OR longitude Is Null OR
       latitude = 0 OR longitude = 0)
"""

update_geographic_location_lat_long_sql="""
UPDATE Geographic_locations
SET latitude = ?, longitude = ?
WHERE geographic_id = ?
"""

#
# Classes
#

from HTMLParser import HTMLParser

class href_parser(HTMLParser):
    def __init__(self,search_list):
        HTMLParser.__init__(self)
        self.href_map={}
        self.search_list=search_list
        self.last_href=''
        self.last_key=''

    def handle_starttag(self, tag, attrs):
        #print "start tag:", tag, attrs
        if tag in self.search_list:
            for name,value in attrs:
                if name=='href': self.last_href=value                

    def handle_endtag(self, tag ):
        #print "end tag:", tag
        if tag in self.search_list and self.last_key:
            self.href_map[self.last_key]=self.last_href
            
    def handle_data(self, data ):
        #print "data:", data
        self.last_key=data

class desc_parser(HTMLParser):
    def __init__(self,start_desc,end_desc,start_task):
        HTMLParser.__init__(self)
        self.description=''
        self.tasks=[]
        self.start_desc=start_desc
        self.end_desc=end_desc
        self.start_task=start_task
        self.task_line=False
        self.last_match=''
        self.last_data=''
        self.curr_line=0

    def handle_starttag(self, tag, attrs):
        #print "start tag:", tag, attrs
        if tag == 'li':
            for name,value in attrs:
                if name=='type' and value=='disc': self.task_line=True               

    def handle_endtag(self, tag ):
        #print "end tag:", tag
        if tag == 'li' and self.task_line:
            self.task_line=False
            
    def handle_data(self, data ):
        # Do something with the data line
        line,pos=self.getpos()
        if line <> self.curr_line:
            self.last_data=self.last_data.strip()
            # process the last lot of data
            #print "data:", line, self.last_data  
            if self.last_data.find(self.start_desc)+1:
                self.last_match='description'
                #print 'Turn on description:', self.last_data
            elif self.last_data.find(self.end_desc)+1:
                self.last_match=''
            elif self.last_data == self.start_task:
                self.last_match='task_list'
                #print 'Turn on task list:', self.last_data
            elif self.last_data and self.last_data[-1]==':':
                self.last_match=''
            elif self.last_data:
                if self.last_match=='description':
                    #print "desc:", self.last_data[:50]
                    if not bad_description_regex.search(self.last_data):
                        if self.description:
                            self.description+=os.linesep+self.last_data
                        else:
                            self.description=self.last_data
                if self.last_match=='task_list' and self.task_line:
                    self.tasks.append(self.last_data)
                    self.task_line=False
            # start with new data for the new line
            self.curr_line=line
            self.last_data=data
        else:
            # still on the same line
            self.last_data+=data



class filter_parser(HTMLParser):
    def __init__(self,start_string,end_string):
        HTMLParser.__init__(self)
        self.result=[]
        self.start_desc=start_string
        self.end_desc=end_string
        self.header=False
        self.last_match=''
        self.last_data=''
        self.curr_line=0

    def handle_starttag(self, tag, attrs):
        #print "start tag:", tag, attrs
        if tag == 'h2':
            self.header=True

    def handle_endtag(self, tag ):
        #print "end tag:", tag
        if tag == 'h2':
            self.header=False
            
    def handle_data(self, data ):
        # Do something with the data line
        line,pos=self.getpos()
        if line <> self.curr_line:
            self.last_data=self.last_data.strip()
            # process the last lot of data
            #print "data:", line, self.last_data  
            if self.last_data.find(self.start_desc)+1:
                self.last_match='matched'
                print 'Turn on filter:', self.last_data
            elif self.last_data.find(self.end_desc)+1:
                self.last_match=''
                print 'Turn off filter:', self.last_data
            elif self.last_data:
                if self.last_match=='matched':
                    print 'matched:', self.last_data
                    self.result.append(self.last_data)
            # start with new data for the new line
            self.curr_line=line
            self.last_data=data
        else:
            # still on the same line
            self.last_data+=data
                
class occ_parser(HTMLParser):
    def __init__(self,start_test,start_nec,skip_list,code_regex):
        HTMLParser.__init__(self)
        self.job_titles={}
        self.job_descriptions={}
        self.start_test=start_test    # eg. 'Occupations:'
        self.start_nec=start_nec      # eg. 'Occupations in this group include:'
        self.skip_list=skip_list      # eg. 'Alternative Title:'
        self.code_regex=code_regex    # eg. anzsco_code_regex
        self.last_code=''
        self.last_nec=''
        self.last_match=''
        self.last_data=''
        self.indented=False
        self.indented_line=False
        self.curr_line=0
        self.skip_step=0

    def handle_starttag(self, tag, attrs):
        #print "start tag:", tag, attrs
        if tag == 'ul':
            self.indented=True
            self.indented_line=self.curr_line          

    def handle_endtag(self, tag ):
        #print "end tag:", tag
        if tag == 'ul' and self.indented:
            self.indented_line=self.curr_line
            self.indented=False
            
    def handle_data(self, data ):
        # Do something with the data line
        line,pos=self.getpos()
        if line <> self.curr_line:
            # process the last lot of data
            self.last_data=self.last_data.strip()
            #print self.curr_line, self.last_data
            # what state are we in?
            if self.start_test.search(self.last_data):
                self.last_match='job names'     # This says we are scanning possible job names and descriptions
                #print 'Turn on:', self.last_data
            elif self.start_nec.search(self.last_data):
                self.last_match='nec stuff'     # This says we are adding extra job titles to a description
                #print 'NEC stuff:', self.last_nec, self.last_data
                if self.last_nec:
                    s,t=self.job_descriptions.get(self.last_nec,['',[]])
                    self.job_descriptions[self.last_nec]=(s+os.linesep+self.last_data+os.linesep,t)
            elif self.last_data and self.last_data.find(':')+1:
                if self.last_code and self.skip_list:
                    for i,regtst in enumerate(self.skip_list,1):
                        if regtst.search(self.last_data):
                            #print 'Skip:', self.last_data, i
                            self.skip_step=i
                            if i>1 and self.last_code:
                                self.last_code=''
            elif self.last_data:
                search=self.code_regex.search(self.last_data)
                # Do we have an ANZSCO code and Job Name?
                if search and self.last_match=='job names':
                    # we have an anzsco code and job title
                    self.skip_step=0
                    self.last_code=self.last_data[search.start():search.end()]
                    print 'Code:', self.last_code
                    if self.last_code not in self.job_titles:
                        # we have a new ANZSCO code
                        self.job_titles[self.last_code]=self.last_data[search.end():].strip()
                    # is this a nec code?
                    if self.last_code[-2:]=='99':
                        self.last_nec=self.last_code
                    else:
                        self.last_nec=''
                # Do we have a NEC extra job titles to add to the description?
                elif not search and self.last_match=='nec stuff' and self.last_nec:
                    #print 'NEC:', self.last_nec, self.last_data
                    s,t=self.job_descriptions.get(self.last_nec,['',[]])
                    self.job_descriptions[self.last_nec]=(s+'  '+self.last_data+os.linesep,t)
                # Do we have more data to add to the job description?
                elif not search and not self.skip_step and self.last_code:
                    # add to the description of this job
                    s,t=self.job_descriptions.get(self.last_code,['',[]])
                    self.job_descriptions[self.last_code]=(s+self.last_data,t)
                    #print 'desc:', s+self.last_data
                # Are we just into data that doesn't belong anywhere?
                elif not search and not self.indented and self.indented_line<>line:
                    # we have ended this
                    #print 'Skip off:', self.last_data, 0
                    self.skip_step=0                
            # start with new data for the new line
            self.curr_line=line
            self.last_data=data
        else:
            # still on the same line
            self.last_data+=data

                
class fld_parser(HTMLParser):
    def __init__(self,broad_field,start_test,start_examples,skip_test,code_regex):
        HTMLParser.__init__(self)
        self.broad=broad_field       # the 4 letter code we are searching for
        self.field_titles={}
        self.field_descriptions={}
        self.start_test=start_test    # eg. 'This narrow field comprises the following detailed'
        self.start_ex=start_examples  # eg. 'Examples of subjects in this detailed field include:'
        self.skip_test=skip_test      # eg. 'Exclusions:'
        self.code_regex=code_regex    # eg. detailed_field_code_regex
        self.last_code=''
        self.last_nec=''
        self.last_match=''
        self.last_data=''
        self.indented=False
        self.indented_line=False
        self.curr_line=0
        self.skip_step=0

    def handle_starttag(self, tag, attrs):
        #print "start tag:", tag, attrs
        if tag == 'ul':
            self.indented=True
            self.indented_line=self.curr_line          

    def handle_endtag(self, tag ):
        #print "end tag:", tag
        if tag == 'ul' and self.indented:
            self.indented_line=self.curr_line
            self.indented=False
            
    def handle_data(self, data ):
        # Do something with the data line
        line,pos=self.getpos()
        if line <> self.curr_line:
            # process the last lot of data
            self.last_data=self.last_data.strip().decode('ascii','ignore')
            #print self.curr_line, self.last_data
            # what state are we in?
            if self.start_test.search(self.last_data):
                self.last_match='field names'     # This says we are scanning possible job names and descriptions
                print 'Turn on:', self.last_data
            elif self.start_ex.search(self.last_data):
                self.last_match='examples'     # This says we are scanning a list of examples
                print 'Example on:', self.last_data
                if self.last_code:
                    s=self.field_descriptions.get(self.last_code,'')
                    self.field_descriptions[self.last_code]=s+os.linesep+self.last_data+os.linesep
            elif self.last_data and self.last_code and self.skip_test.search(self.last_data):
                print 'Skip:', self.last_data, 1
                self.last_code=''
                self.skip_step=1
            elif self.last_data:
                search=self.code_regex.search(self.last_data)
                # Do we have an Field code and Field Name?
                if search and self.last_match:
                    # we have an field code and field title
                    self.last_match='field names'
                    self.skip_step=0
                    self.last_code=self.last_data[search.start():search.end()].strip()
                    print 'Code:', self.last_code
                    if self.last_code not in self.field_titles and self.last_code.startswith(self.broad):
                        # we have a new Field of education code, strip spaces and non-ascii characters
                        self.field_titles[self.last_code]=self.last_data[search.end():].strip()
                    # is this a nec code?
                    if self.last_code[-2:]=='99':
                        self.last_nec=self.last_code
                    else:
                        self.last_nec=''
                # Do we have some examples to add to the description? cnverts seperate lines to commas
                elif not search and self.last_match=='examples' and self.last_code:
                    print 'Example:', self.last_code, self.last_data
                    s=self.field_descriptions.get(self.last_code,'')
                    self.field_descriptions[self.last_code]=s+', '+self.last_data.strip('.').strip()
                # Do we have more data to add to the job description?
                elif not search and not self.skip_step and self.last_code:
                    # add to the description of this job
                    s=self.field_descriptions.get(self.last_code,'')
                    self.field_descriptions[self.last_code]=s+self.last_data.strip()
                    print 'desc:', s+self.last_data
                    if self.last_nec:
                        self.last_match=''
                # Are we just into data that doesn't belong anywhere?
                elif not search:
                    # we have ended this
                    print 'Skip off:', self.last_data, 0
                    self.skip_step=0
            # start with new data for the new line
            self.curr_line=line
            self.last_data=data
        else:
            # still on the same line
            self.last_data+=data
            
#
# Functions
#

def test_urllib(url_str='http://google.com.au'):
    #
    # Given a page search the source for the list of industries and extract all the letter - industry pairs
    # eg. { 'A':'Agriculture, Forestry, Fishing', 'B':'Mining' }    
    #page=urllib.urlopen(url_str)
    # example from website: http://docs.python.org/howto/urllib2.html
    req = urllib2.Request(url_str)
    response = urllib2.urlopen(req)
    lines=response.readlines()
    response.close()
    #
    print url_str
    print len(lines)
    #
    for line in lines[:50]:
        print line[:50].strip()

def clean_value(s):
    #
    # remove any spurious html coding in the value
    # eg. <acronym title=Information and Communication Technology">ICT</acronym> Managers -> ICT Managers
    # Also strip any '"' characters
    #
    global all_tag_pairs
    s=s.encode('ascii','ignore').replace('"','')
    length=len(s)
    for tag,rexs in sorted(all_tag_pairs.items()):
        a,b=rexs
        matchs=a.search(s)
        matche=b.search(s)
        if matchs and matche:
            #print matchs.start(), matche.start(), s[matchs.start():matche.start()]
            pos=s[:matche.start()].rfind('>')+1
            if pos:
                piece=s[pos:matche.start()]
            result=piece+s[matche.end():]
        else:
            #print "No match found", k, 'in', s
            result=s[:]
        s=result
    return result

def fetch_html_list_map(url_str,list_start_str,list_end_str,search_key='industry',html_data={}):
    #
    # Given a page search the source for the list of industries and extract all the letter - industry pairs
    # eg. { 'A':'Agriculture, Forestry, Fishing', 'B':'Mining' }
    #
    # check if we have html data to encode
    #
    result={}
    #
    if html_data:
        # encode the data
        print url_str
        print html_data
        print 'sleeping (1)...'
        time.sleep(1)
        encoded = urllib.urlencode(html_data)
        try:
            req = urllib2.Request(url_str,encoded)
        except urllib2.URLError:
            print 'ERROR: Something went wrong requesting this url and data'
            print '      ', url_str
            print '      ', html_data
            return result   # empty
    else:
        # no data to add
        print url_str
        try:
            req = urllib2.Request(url_str)
        except urllib2.URLError:
            print 'ERROR: Something went wrong requesting this url'
            print '      ', url_str
            return result   # empty
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError:
        print 'ERROR: Something went wrong opening this url'
        print '      ', url_str
        return result   # empty
    lines=response.readlines()
    response.close()
    print len(lines)
    print list_start_str
    #
    # first extract the text with the industries listed
    data,started='',False
    count=0
    for line in lines:
        count+=1
        # Check if the line contains what we need
        pos=line.find(list_start_str)+1
        #print pos
        #
        if not started and pos:
            # we have found the specific industry listing
            started=True
            print count,started,pos
            data=line[pos:]
            # does the line end here as well?
            pos=data.find(list_end_str)+1  
            if pos:
                # we have found the end
                started=False
                data=data[:pos+len(list_end_str)-1]
        #
        # check for multi-line data
        if started:
            # we are searching for the end of the list
            pos=line.find(list_end_str)+1
            if pos:
                # we have found the end
                started=False
                data+=line[:pos+len(list_end_str)-1]
            else:
                data+=line
        #
        #
        # check for ending
        if data and not started:
            print data[:70]
            print '...'
            print data[-30:]
            break
    # End of For
    #
    if data:
        data_length=len(data)
##        print
##        print data_length
        str1,str2,str3=search_strings[search_key]
        len1,len2,len3=len(str1),len(str2),len(str3)
        pos=0
        #
##        print data_length
##        print str1,str2,str3
##        print len1,len2,len3
##        print
        #
        while pos < data_length:
            #print pos
            #
            # search for the 3 place holders
            pos1=data[pos:].find(str1)+pos
            pos2=data[pos1:].find(str2)+pos1
            pos3=data[pos2:].find(str3)+pos2
            #
            #print pos,pos1,pos2,pos3
            #        
            k=data[pos1+len1:pos2]
            v=data[pos2+len2:pos3]
            if k and v:
                result[k]=clean_value(v)[:]
                #print k,'=',result[k]
                if v.find('<')+1 or v.find('</')+1:
                    print "WARNING: found more html tags in value:", v[v.find('<'):v.find('>',v.find('<'))]                                                                      
            else:
                #print '  Found nothing.'
                pos=data_length
            #
            if pos3+1 and pos3 > pos:
                pos=pos3+len3+1
            else:
                pos=data_length
    else:
        # problem
        print "ERROR: found no data for page -", url_str
        print "   search string:", list_start_str
        raise RuntimeError
    #
    return result   

def reload_industry_occupations_table(M,N):
    #
    # retrieve the access database to be used
    save_dir=os.getcwd()    # remember where we are now
    filename=EasyDialogs.AskFileForOpen('Select the Access DB to be imported from',
                                        typeList=default_db_typelist,
                                        defaultLocation=default_location)
    # have we changed directory?
    if save_dir <> os.getcwd():
        os.chdir(save_dir)  # change back to where we were
    #
    if not filename:
        filename=default_source_file
    #
    connect_str=access_connect_str % filename
    print connect_str
    #
    db=pyodbc.connect(connect_str)
    c=db.cursor()
    #
    # Build a list from the data maps
    data=[]
    #
    for k,jobs in sorted(N.items()):
        industry=M[k]
        for code,desc in jobs.items():
            data.append([k,industry,code,desc])
    #
    if not data:
        raise RuntimeException
    #
    print 'data found', len(data)
##    for i in range(5):
##        print '  ',data[i]
    print
    #
    print 'Executing:\n  ',
    print delete_industry_occupations_sql
    c.execute(delete_industry_occupations_sql)
    #
    print 'Inserting...'
    for each in data:
        #print '.',
        #print '  ',each
        #print c.execute(insert_industry_occupations_sql,each)
        c.execute(insert_industry_occupations_sql,each)
    #
    # cleanup
    db.commit()
    c.close()
    db.close()

def dump_industry_data_to_files(M,N):
    #
    #   Given two maps dump the data to 2 files
    #
    if not M:
        raise RuntimeError
    #
    output=open('industries.csv','w')
    output.write('code,description\n')
    for k,v in sorted(M.items()):
        output.write('%s,"%s"\n' % (k,v))
    output.close()
    #
    if not N:
        raise RuntimeException
    #
    output=open('industry_occupations.csv','w')
    output.write('industry,job_code,description\n')
    for k,jobs in sorted(N.items()):
        for code,desc in jobs.items():
            output.write('%s,%s,"%s"\n' % (k,code,desc))
    output.close()
    
def industry_occupations(reload_database=True,dump_to_files=True):
    #
    # Everything to do with fetching industry to occupation code relationships from the Job Outlook website
    global industry_map, industryspecific_map
    #
    # retrieve the top level page to extract the industries
    industry_map=fetch_html_list_map(industry_page,industry_list_start_str,list_end_str,'industry')
    #
    # Now for each industry extract the specific job descriptions
    for key,industry_name in sorted(industry_map.items()):
        #
        print
        print key,' = ',industry_name
        #print '"%s"' % (industry_specific_page+key)
        #print
        #
        html_data={'search':'industry','industry':key}
        industryspecific_map[key]={}    # start ith an empty list for this industry
        #
        this_map=fetch_html_list_map(industry_specific_page+key,specific_jobs_start_str,list_end_str,'specific',html_data)
        print key, len(this_map)
        #
        industryspecific_map[key].update(this_map)
        this_map={}
        #
        for code,job in industryspecific_map[key].items():
            #
            print '>   ',code,' = ',repr(job)
    #
    # reload file by choice
    if reload_database:
        reload_industry_occupations_table(industry_map,industryspecific_map)
    #
    # output to .csv files
    if dump_to_files:
        dump_industry_data_to_files(industry_map,industryspecific_map)
    #

def fetch_html_lines(url_str,html_data=None,filter=None):
    """
    Given a page and some data, return the html lines
    """
    result=[]
    #
    if html_data:
        # encode the data
        print url_str
        print html_data
        print 'sleeping (1)...'
        time.sleep(1)
        encoded = urllib.urlencode(html_data)
        try:
            print url_str
            print encoded
            req = urllib2.Request(url_str,encoded)
        except urllib2.URLError:
            print 'ERROR: Something went wrong requesting this url and encoding'
            return result   # empty
    else:
        # no data to add
        print url_str
        try:
            req = urllib2.Request(url_str)
        except urllib2.URLError:
            print 'ERROR: Something went wrong requesting this url'
            return result   # empty
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError:
        print 'ERROR: Something went wrong opening this url'
        return result   # empty
    lines=response.readlines()
    response.close()
    #
    # do we filter the lines some more?
    if filter:
        print len(lines)
        start_regex,end_regex=filter
        started=False
        for line in lines:
            if start_regex.search(line):
                started=True
            if started:
                result.append(line)
            if end_regex.search(line):
                started=False
        if result:
            lines=result
##        start_str,end_str=filter
##        print 'Filetring page (%d) with (%s,%s).' % (len(lines),start_str,end_str)
##        P=filter_parser(start_regex,end_regex)
##        for line in lines:
##            P.feed(line)
##        lines=P.result
    #
    return lines

def fetch_job_prospects(code):
    """
    Go to the website and extract the prospects paragraph
    """    
    shortcode=code.strip('0')
    data={}
    data['code']=shortcode
    data['search']='alpha'
    data['Tab']='prospects'
    #
    #lines=fetch_html_lines(job_outlook_page,data,('Job Prospects','Key Indicators'))
    #lines=fetch_html_lines(job_prospects_prefix+code,filter=('Job Prospects','Key Indicators'))
    olines=fetch_html_lines(job_overview_page % int(shortcode),filter=parsing_prospects_filters['overview'])
    print len(olines)
    plines=fetch_html_lines(job_prospects_page % int(shortcode),filter=parsing_prospects_filters['prospects'])
    print len(plines)
    return olines,plines
    
def job_outlook_prospects(digits=4,reload_database=True,dump_to_files=True,target=default_target_file):
    """
    Go to the job_outlook website and download job prospects data
    """
    global anzsco_descriptions
    global anzsco_titles
    #
    # retrieve the access database to be used
    if not target:
        save_dir=os.getcwd()    # remember where we are now
        target=EasyDialogs.AskFileForOpen('Select the Access DB to be imported from',
                                            typeList=default_db_typelist,
                                            defaultLocation=default_location)
        # have we changed directory?
        if save_dir <> os.getcwd():
            os.chdir(save_dir)  # change back to where we were
    #
    connect_str=access_connect_str % target
    print connect_str
    #
    db=pyodbc.connect(connect_str)
    c=db.cursor()
    #
    # the job_code list is determined by the general
    outside='%'+(6-digits)*'0'
    inside='%'+(7-digits)*'0'
    sql_stmt = select_job_codes_sql % (outside,inside)
    #
    # fetch the list of job codes
    print 'Executing...'
    print sql_stmt   
    c.execute(sql_stmt)
    for each in c.fetchall():        
        code=str(each.anzsco)
        title=str(each.job_name)
        desc=str(each.description)
        print code, title
        anzsco_titles[code]=title
        anzsco_descriptions[code]=desc
        #
        fetch_job_prospects(code)
        #fetch_job_earnings(code)
    #
    # cleanup
    c.close()
    db.commit()
    db.close()
    
          
def clean_str( s ):
    """
    remove offending characters from a string
    """
    result=''
    if hasattr(s,'decode') and hasattr(s,'strip'):
        s=s.decode('latin-1','ignore').strip()
    if s:
        result=s.replace('&','and')
    return result

def clean_url_str( s ):
    """
    remove offending characters from a string
    """
    return s.replace(' ','+')

def fetch_latitude_longitude(address):
    """
    Given an address string attempt to use the Google API to fetch the latitude and longitude
    """
    google_api_url="http://%s/%s?address=%s&sensor=false" % (google_map_api,google_map_data_type,clean_url_str(address))
    #
    req = urllib2.Request(google_api_url)
    response = urllib2.urlopen(req)
    X=json.load(response)
    response.close()
    #
    if X and 'status' in X and X['status'] == "OK":
        result=X['results'][0]['geometry']['location']
        print result
    else:
        result=None
        print "Invalid Address for Google Maps", address
        if X and 'status' in X:
            print X['status']
            if X['status'] == 'OVER_QUERY_LIMIT':
                raise RuntimeError
        return None,None
    #
    if 'lat' in result and 'lng' in result:
        return (result['lat'],result['lng'])
    else:
        return None,None

def load_all_delivery_location_latlong(target_db=default_target_file,limit=20):
    """
    Scroll through all delivery locations and try to generate a latitude longitude value for each one
    """
    connect_str=access_connect_str % target_db
    print "Updating DB:", connect_str
    #
    db=pyodbc.connect(connect_str)
    c=db.cursor()
    #
    # Build a list from the data maps
    locations={}
    sites={}
    #
    print 'Executing:\n  ',
    print select_delivery_locations_sql
    c.execute(select_delivery_locations_sql)
    #
    print 'fetching...'
    for each in c.fetchall()[:limit]:
        #
        print each[:3]
        campus_id,asqa,site,add1,add2,loc,pcode,state = list(each)
        #
        if not campus_id:
            continue
        #
        add1 =clean_str(add1)
        add2 =clean_str(add2)
        loc  =clean_str(loc)
        pcode=clean_str(pcode)
        state=clean_str(state)
        #
        sites[campus_id]=(asqa,site)
        #
        # fetch the lat long
        if add1:
            # we have to have some address
            address = ' '.join([add1,add2,loc,pcode,state,'AUSTRALIA'])
            print address
            result = fetch_latitude_longitude(address)
            print result
        else:
            result = None
        #
        # check, did it work
        if result and len(result) == 2:
            locations[campus_id]=result
            print campus_id, result
    #
    # Save the results    
    print "campus_id,asqa_code,site_name,latitude,longitude"
    for k,v in sorted(locations.items()):
        #
        print '%d,"%s","%s",%.6f,%.6f' % ((k,)+sites[k]+v)
        c.execute(update_delivery_locations_sql,(v[0],v[1],k))
    #
    # report success!
    print
    print '%d Delivery Locations have been updated with Latitude and Longitude.' % len(locations)
    print 'Done.'
    print
    #
    # cleanup
    db.commit()
    c.close()
    db.close()

def fetch_abs_occupation_details(website,anzsco_code,key,level):
    """
    Given an azsco_code, key, and parser object, find the document from the parser href map
    and open the page and retrieve more pasge to load, further titles, descriptions and task lists
    """
    global anzsco_descriptions
    global anzsco_titles
    pages={}
    #key_upper=key.upper()
    #
    print anzsco_code, key, level
    try:
        req = urllib2.Request(abs_occupation_prefix+website)
    except urllib2.URLError:
        print 'ERROR: Something went wrong requesting this url', abs_occupation_prefix+website
        return result   # empty
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError:
        print 'ERROR: Something went wrong opening this url', abs_occupation_prefix+website
        return result   # empty
    lines=response.readlines()
    response.close()
    #
    # extract the description:
    #     - key_upper -> description ->
    key_used=key.upper()
    title_upper_regex=re.compile(key.upper())
    deslines=[]
    started=False
    for line in lines:
        if title_upper_regex.search(line):
            started=True
        if started:
            deslines.append(line)
    #
    if not deslines:
        print "WARNING couldn't find the exact title match, search by ANZSCO code"
        key_used=abs_level_map[level].upper()+' '+anzsco_code.strip('0')
        alt_key_regex=re.compile(key_used)
        title_started=False
        for line in lines:
            if start_title_regex.search(line):
                title_started=True
            if title_started and alt_key_regex.search(line):
                started=True
            if started:
                deslines.append(line)
    #
    # create a parser object to extract the description and the task list
    print anzsco_code, len(deslines), key_used
    print 'Parsing description extract:'
    D=desc_parser(key_used,'Indicative Skill Level:','Tasks Include:')
    for line in deslines:
        #print line
        D.feed(line)
        if chapitem_regex.search(line):
            break
    print D.description
    print
    print 'Tasks:'
    for each in D.tasks:
        print each
    description=(D.description.replace(os.linesep+os.linesep,os.linesep),D.tasks)
    #anzsco_titles[anzsco_code]=key
    # cleanup double line feeds
    anzsco_descriptions[anzsco_code]=description
    if level >= 4:
        #
        # Scan for the final 6 digit occupations, collect there titles and short descriptions
        print
        print 'Parsing unit level titles and descriptions:', len(deslines)
        Occ=occ_parser(occupations_regex,nec_occupations_regex,parsing_occupation_skip_list,anzsco_code_regex)
        for line in deslines:
            # stop when we reach a navigate button, i.e. the end of the data
            if chapitem_regex.search(line):
                print 'Found a chapitem, exiting line scan'
                break
            #print line
            Occ.feed(line)
        print Occ.job_descriptions
        print Occ.job_titles
        # add unit level descriptions, eg. 351111 Baker -> Prepares and bakes bread loaves and rolls.
        anzsco_descriptions.update(Occ.job_descriptions)
        # add unit level titles, eg. 351111 Baker
        anzsco_titles.update(Occ.job_titles)
    #
    # extract the sub pages, filtering on chapitem tags and parsing for the href data
    P=href_parser(abs_subpages_tags)
    sublines=filter(chapitem_regex_filter,lines)
    print 'Parsing sub pages:'
    print len(sublines)
    for line in sublines:
        #print line
        P.feed(line)
    #
    # cleanup
    del lines, sublines, deslines
    #
    # Decide on scanning for the next level
    if level < 4 and P.href_map:
        # Scan down to the next level
        level+=1
        prefix=abs_level_map[level]
        prefix_len=len(prefix)
        prefix_upper=prefix.upper()
        print prefix_len, level, prefix_upper
        for key in sorted(filter(lambda x: x[:prefix_len].upper()==prefix_upper, P.href_map.keys())):
            anzsco_frag=key[prefix_len+1:prefix_len+1+level]
            anzsco_title=key[prefix_len+level+1:]
            anzsco_code=string.ljust(anzsco_frag,6,'0')
            anzsco_titles[anzsco_code]=anzsco_title.strip()    
            print key, anzsco_frag, anzsco_code
            subpages=fetch_abs_occupation_details(P.href_map[key],anzsco_code,key,level)
            pages.update(subpages)
            # for debugging
            #if level < 3: break
    #
    return pages
    
def fetch_abs_occupation_top(website,start_point):
    """
    Using a website string for the open document destination trigger, and the start point,
    cycle through all the Major group content, and read the descriptions form the child pages
    """
    global anzsco_descriptions
    global anzsco_title
    pages={}
    anzsco_descriptions={}
    #
    print start_point
    try:
        req = urllib2.Request(website+start_point)
    except urllib2.URLError:
        print 'ERROR: Something went wrong requesting this url'
        return result   # empty
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError:
        print 'ERROR: Something went wrong opening this url'
        return result   # empty
    lines=response.readlines()
    response.close()
    #
    lines=filter(chapitem_regex_filter,lines)
    print len(lines)
    #
    # create a parser object
    P=href_parser(abs_subpages_tags)
    #
    # parse the starting page, adding all the new  sub pages
    print 'Parsing sub pages:'
    for line in lines:
        #print line
        P.feed(line)
    #
    # Find the links to the sub pages at this level (Major group)
    level=1
    prefix='Major Group'
    prefix_len=len(prefix)
    prefix_upper=prefix.upper()
    print prefix_len, level, prefix_upper
    for key in sorted(filter(lambda x: x[:prefix_len].upper()==prefix_upper, P.href_map.keys())):
        anzsco_frag=key[prefix_len+1:prefix_len+level+1]
        anzsco_title=key[prefix_len+level+2:]
        anzsco_code=string.ljust(anzsco_frag,6,'0')
        anzsco_titles[anzsco_code]=anzsco_title.strip()
        print key, anzsco_frag, anzsco_code
        subpages=fetch_abs_occupation_details(P.href_map[key],anzsco_code,key,level)
        pages.update(subpages)
        # for debugging
        #break
    #
    # verify and check titles against descriptions
    for code,title in sorted(anzsco_titles.items()):
        if code not in anzsco_descriptions:
            print 'WARNING: anzsco code - %s - has no description (%s)' % (code,title)
        else:
            # Correct any UPPER CASE job titles
            desc,tasks=anzsco_descriptions[code]
            anzsco_descriptions[code]=(desc.replace(title.upper(),title),tasks)
    #
    return pages

def reload_occupations_descriptions():
    #
    #   Given the global maps for anzsco descriptions and tasks insert these into the DB
    #
    global anzsco_descriptions
    global anzsco_titles
    #
    # retrieve the access database to be used
    save_dir=os.getcwd()    # remember where we are now
    filename=EasyDialogs.AskFileForOpen('Select the Access DB to be imported from',
                                        typeList=default_db_typelist,
                                        defaultLocation=default_location)
    # have we changed directory?
    if save_dir <> os.getcwd():
        os.chdir(save_dir)  # change back to where we were
    #
    if not filename:
        filename=default_target_file
    #
    connect_str=access_connect_str % filename
    print connect_str
    #
    db=pyodbc.connect(connect_str)
    c=db.cursor()
    #
    print 'Updating anzsco decsriptions...'
    print update_occupation_description_sql
    for anzsco,v in anzsco_descriptions.items():
        description,tasks=v
        short_description=description
        if tasks:
            # we have a group level description, include the tasks.
            description+=os.linesep+'Tasks Include:'+os.linesep
            for task in tasks:
                description+=' - '+task+os.linesep
        elif anzsco[-1:]<>'0':
            # we have a unit level description, include the tasks from the group above
            group_anzsco=anzsco[:4]+'00'
            if group_anzsco in anzsco_descriptions:
                # add the tasks from this group to the low level description
                tasks=anzsco_descriptions[group_anzsco][1]            
                description+=os.linesep+'Tasks Include:'+os.linesep
                for task in tasks:
                    description+=' - '+task+os.linesep
        else:
            pass
        print anzsco, description[:30]
        c.execute(update_occupation_description_sql,(description,anzsco))
        if anzsco[-1:]<>'0':
            # At unit level give the alternative titles and specialisations the shorter description
            c.execute(update_occupation_short_description_sql,(short_description,anzsco))

    #
    # cleanup
    db.commit()
    c.close()
    db.close()

def dump_occupation_data_to_files():
    #
    #   Given a maps dump the data to a files for descriptions and a file for tasks
    #
    global anzsco_descriptions
    global anzsco_titles
    #
    if not anzsco_descriptions or not anzsco_titles:
        raise RuntimeError
    M=anzsco_descriptions
    #
    output=open('occupation_descriptions.csv','w')
    output.write('anzsco code,job title,description\n')
    for k,v in sorted(M.items()):
        if k in anzsco_titles:
            output.write('"%s","%s","%s"\n' % (k,anzsco_titles[k],v[0]))
        else:
            output.write('"%s","%s","%s"\n' % (k,'??????',v[0]))
    output.close()
    #
    output=open('occupation_tasks.csv','w')
    output.write('anzsco code,task number,task description\n')
    for k,v in sorted(M.items()):
        for task,desc in enumerate(v[1],1):
            output.write('"%s",%d,"%s"\n' % (k,task,desc))
    output.close()

def occupation_descriptions(reload_database=True,dump_to_files=True):
    """
    Go to the ABS website and retrieve the occupation descriptions
    """
    #
    # Everything to do with fetching industry to occupation code relationships from the Job Outlook website
    global occupation_map
    #
    # retrieve the top level page to extract the industries
    occupation_map=fetch_abs_occupation_top(abs_occupation_prefix,abs_occupation_contents_table)
    #
    # reload file by choice
    if reload_database:
        reload_occupations_descriptions()
    #
    # output to .csv files
    if dump_to_files:
        dump_occupation_data_to_files()
    #

def fetch_abs_field_details(website,field_code,key,level):
    """
    Given an field code, key, and parser object, find the document from the parser href map
    and open the page and retrieve more pasge to load, further field of education titles, and descriptions
    """
    global field_descriptions
    global field_titles
    pages={}
    #key_upper=key.upper()
    #
    print field_code, key, level
    try:
        req = urllib2.Request(abs_fields_prefix+website)
    except urllib2.URLError:
        print 'ERROR: Something went wrong requesting this url', abs_fields_prefix+website
        return result   # empty
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError:
        print 'ERROR: Something went wrong opening this url', abs_fields_prefix+website
        return result   # empty
    lines=response.readlines()
    response.close()
    #
    # extract the description:
    #     - key_upper -> description ->
    key_used=key.upper()
    title_upper_regex=re.compile(key_used)
    deslines=[]
    started=False
    for line in lines:
        if title_upper_regex.search(line):
            started=True
        if started:
            deslines.append(line)
    #
    if not deslines:
        print "WARNING couldn't find the exact title match, search by Field code"
        key_used=abs_field_level_map[level].upper()+' '+field_code
        other_key_used=abs_field_level_map[level].upper()+'\xa0'+field_code
        alt_key_regex=re.compile(key_used+'|'+other_key_used)
        title_started=False
        for line in lines:
            if start_title_regex.search(line):
                title_started=True
            if title_started and alt_key_regex.search(line):
                started=True
            if started:
                deslines.append(line)
    #
    # create a parser object to extract the description and the task list
    print field_code, len(deslines), key_used
    print 'Parsing description extract:'
    D=desc_parser(abs_field_level_map[level].upper(),'This broad field','This narrow field')
    for line in deslines:
        if chapitem_regex.search(line):
            break
        #print line
        D.feed(line)
    print D.description
    # cleanup double line feeds
    description=D.description.replace(os.linesep+os.linesep,os.linesep)
    field_descriptions[field_code]=description
    if level >= 2:
        #
        # Scan for the final 6 digit fields, collect their titles and short descriptions
        print
        print 'Parsing narrow level titles and descriptions:', len(deslines)
        Fld=fld_parser(field_code,detailed_field_list_regex,examples_field_regex,exclusions_regex,detailed_field_code_regex)
        for line in deslines:
            #print line
            Fld.feed(line)
            # stop when we reach a navigate button, i.e. the end of the data
            if chapitem_regex.search(line):
                #print 'Found a chapitem, exiting line scan'
                break
        #print Fld.job_descriptions
        #print Fld.job_titles
        # add detailed level titles, eg. 010103 Statistics
        field_titles.update(Fld.field_titles)
        # add detailed level descriptions, eg. 010103 Statistics -> collecting, describing, arranging and analysing numerical data
        for k,d in Fld.field_descriptions.items():
            field_descriptions[k]=d.replace(':'+os.linesep+',',':')\
                                   .replace('This detailed field includes','This field includes')
    #
    # extract the sub pages, filtering on chapitem tags and parsing for the href data
    P=href_parser(abs_subpages_tags)
    sublines=filter(chapitem_regex_filter,lines)
    print 'Parsing sub pages:'
    print len(sublines)
    for line in sublines:
        #print line
        P.feed(line)
    #
    # cleanup
    del lines, sublines, deslines
    #
    # Decide on scanning for the next level
    if level < 2 and P.href_map:
        # Scan down to the next level
        level+=1
        prefix_len=(2*level)
        print prefix_len, level
        for key in sorted(filter(lambda x: narrow_field_code_regex.search(x), P.href_map.keys())):
            field_frag=key[:prefix_len]
            field_title=key[prefix_len+1:]
            field_code=field_frag.strip()
            field_titles[field_code]=field_title.strip().decode('ascii','ignore')
            print key, field_frag, field_code, field_title 
            subpages=fetch_abs_field_details(P.href_map[key],field_code,key,level)
            pages.update(subpages)
            # for debugging
            #if level < 3: break
    #
    return pages
    
def fetch_abs_field_top(website,start_point):
    """
    Using a website string for the open document destination trigger, and the start point,
    cycle through all the Major group content, and read the descriptions form the child pages
    """
    global field_descriptions
    global field_title
    pages={}
    field_descriptions={}
    #
    print start_point
    try:
        req = urllib2.Request(website+start_point)
    except urllib2.URLError:
        print 'ERROR: Something went wrong requesting this url'
        return result   # empty
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError:
        print 'ERROR: Something went wrong opening this url'
        return result   # empty
    lines=response.readlines()
    response.close()
    #
    lines=filter(chapitem_regex_filter,lines)
    print len(lines)
    #
    # create a parser object
    P=href_parser(abs_subpages_tags)
    #
    # parse the starting page, adding all the new  sub pages
    print 'Parsing sub pages:'
    for line in lines:
        #print line
        P.feed(line)
    #
    # Find the links to the sub pages at this level (Broad Field)
    level=1
    prefix_len=(2*level)+1
    print prefix_len, level
    #print P.href_map.keys()
    for key in sorted(filter(lambda x: broad_field_code_regex.search(x), P.href_map.keys())):
        field_frag=key[:prefix_len]
        field_title=key[prefix_len:]
        field_code=field_frag.strip()
        field_titles[field_code]=field_title.strip()
        print key, field_frag, field_code, field_title
        print P.href_map[key]
        print
        subpages=fetch_abs_field_details(P.href_map[key],field_code,key,level)
        pages.update(subpages)
        # for debugging
        #break
    #
    # verify and check titles against descriptions
    for code,title in sorted(field_titles.items()):
        if code not in field_descriptions:
            print 'WARNING: field code - %s - has no description (%s)' % (code,title)
        else:
            # Correct any UPPER CASE job titles
            desc=field_descriptions[code]
            if len(desc) == 2 and len(desc[0]) > 1:
                desc=desc[0]
            field_descriptions[code]=desc.replace(title.upper(),title)\
                                      .replace('\r\r\n','\r\n')\
                                      .replace(title+'\r\n'+title,title)\
                                      .replace('This narrow field includes','This field includes')\
                                      .replace('this narrow field of education','this field of education')
    #
    return pages

def reload_field_descriptions():
    #
    #   Given the global maps for anzsco descriptions and tasks insert these into the DB
    #
    global field_descriptions
    global field_titles
    #
    # retrieve the access database to be used
    save_dir=os.getcwd()    # remember where we are now
    filename=EasyDialogs.AskFileForOpen('Select the Access DB to be imported from',
                                        typeList=default_db_typelist,
                                        defaultLocation=default_location)
    # have we changed directory?
    if save_dir <> os.getcwd():
        os.chdir(save_dir)  # change back to where we were
    #
    if not filename:
        filename=default_target_file
    #
    connect_str=access_connect_str % filename
    print connect_str
    #
    db=pyodbc.connect(connect_str)
    c=db.cursor()
    #
    print 'Updating field descriptions...'
    print update_field_description_sql
    for field,description in field_descriptions.items():
        print field, description[:30]
        c.execute(update_field_description_sql,(description,field))
    #
    # cleanup
    db.commit()
    c.close()
    db.close()

def dump_field_data_to_files():
    #
    #   Given a maps dump the data to a files for descriptions and a file for tasks
    #
    global field_descriptions
    global field_titles
    #
    if not field_descriptions:
        raise RuntimeError
    M=field_descriptions
    #
    output=open('field_descriptions.csv','w')
    output.write('foe code,title,description\n')
    for k,d in sorted(M.items()):
        if k in field_titles:
            output.write('"%s","%s","%s"\n' % (k,field_titles[k],d))
        else:
            output.write('"%s","%s","%s"\n' % (k,'??????',d))
    output.close()

def field_of_education_descriptions(reload_database=True,dump_to_files=True):
    """
    Go to the ABS website and retrieve the occupation descriptions
    """
    #
    # Everything to do with fetching industry to occupation code relationships from the Job Outlook website
    global field_map
    #
    # retrieve the top level page to extract the industries
    field_map=fetch_abs_field_top(abs_fields_prefix,abs_fields_definition_table)
    #
    # reload file by choice
    if reload_database:
        reload_field_descriptions()
    #
    # output to .csv files
    if dump_to_files:
        dump_field_data_to_files()

def extract_titles_from_outline(pdf):
    """
    Useful function for grabbing some structure frm a PDF file
    """
    titles=[]
    try:
        titles=map(lambda x:x['/Title'],pdf.getOutlines())
    except (KeyError,TypeError):
        # Probably means something wasn't a dictionary in this object
        titles=[]
        for each in pdf.getOutlines():
            if hasattr(each,'get'):
                titles.append(each.get('/Title',''))
            elif (type(each)==type([]) or type(each)==type(())) and hasattr(each[0],'get'):
                titles.append(each[0].get('/Title',''))
            else:
                titles.append('')
        titles=filter(None,titles)
    return titles

def extract_from_pdf(path):
    """
    Given a .pdf try and extract a dsecription and any entry requirements
    """
    # defaults
    description=''
    entryreqs=''
    # create the pdf object
    fp=open(path,'rb')
    pdf=pyPdf.PdfFileReader(fp)
    # assume the description is on page #2
    numpages=pdf.getNumPages()
    # extract some document structure
    #titles=map(lambda x:x['/Title'],pdf.getOutlines())
    titles=extract_titles_from_outline(pdf)
    # extract the page text
    pages={}
    for page in range(pdf.getNumPages()):
        pages[page]=pdf.getPage(page).extractText()
    if titles:
        document_name=titles[0]
        sections=[]
        for title in titles:
            # sometimes the title is a dodgy expression
            try:
                R=re.compile(title)
            except:
                # try after removing dodgy stuff
                for i,c in enumerate(title):
                    if c in string.punctuation:
                        title=title[:i]
                        break
                R=re.compile(title)
            for page,text in sorted(pages.items()):
                match=R.search(text)
                if match:
                    sections.append((page,match.start(),match.end(),title))
    # extract the desired sections from the text
    prev_end,prev_page=0,0
    prev_text,prev_section,text='','',''
    result={}
    for page,start,end,title in sorted(sections):
        #print page,start,end,title
        #
        if page <> prev_page:
            # we have moved to a new page, captrue text at the end of the previous page
            prev_text=pages[prev_page][prev_end:]
            text=pages[page]
            prev_end=0
        elif prev_end and text:
            # same page capture previous sections text to the start of the new section
            prev_text=text[prev_end:start]
        # check to see if we just ended the text from another page
        if prev_section and (prev_section <> document_name or not page):
            # we have a new section, and the previous section wasn't the first page/ or a footer
            #print prev_section,'->',prev_text
            result[prev_section]=prev_text
        # what name did we just find
        prev_section=title.upper()
        # remember the page and the end of the section title
        prev_page=page
        prev_end=end
    # record description and entry requirements extracted   
    if description_title in result:
        description=result[description_title].strip().replace('.  ','.\n').replace(':  ',': ').replace('  ',', ')
        # The AUR40105 clause
        description=description.replace('\nJob roles/employment outcomes ','\n')
    if entry_requirements_title in result:
        entryreqs=result[entry_requirements_title].strip()
    #
    # cleanup
    fp.close()
    del pdf
    #
    return (description,entryreqs)

def download_file(url=default_tga_url,filename='',destination_path=default_dest):
    """
    Given a root URL and a filename download the file to the destination directory
    """
    if not filename:
        filename=os.path.basename(url)
    elif filename and not os.path.basename(url):
        url+=filename
    if filename and url:
        if not os.path.dirname(filename):        
            urllib.urlretrieve(url,destination_path+os.sep+filename)
        else:
            urllib.urlretrieve(url,filename)

def reload_geographic_location_latlong():
    """
    Choose a source database and then upload the latitude and longitude for each location
    """
    
    #
    # retrieve the access database to be used
    save_dir=os.getcwd()    # remember where we are now
    filename=EasyDialogs.AskFileForOpen('Select the Access DB to be imported from',
                                        typeList=default_db_typelist,
                                        defaultLocation=default_location)
    # have we changed directory?
    if save_dir <> os.getcwd():
        os.chdir(save_dir)  # change back to where we were
    #
    if not filename:
        filename=default_target_file
    #
    connect_str=access_connect_str % filename
    print connect_str
    #
    db=pyodbc.connect(connect_str)
    c=db.cursor()
    #
    c.execute(select_geographic_location_sql)
    results = c.fetchall()
    for row in results[:1000]:
        #
        geoid,location,pcode,old_lat,old_lng=list(row)
        #
        try:
            lat,lng = fetch_latitude_longitude('%s,%s,Victoria,Australia' % (location,pcode))
        except:
            lat, lng = None, None
            break
        #
        if lat and lng and (old_lat <> lat or old_lng <> lng):
            print 'Updating geographic latitude and longitude for', geoid, location, pcode
            c.execute(update_geographic_location_lat_long_sql,(lat,lng,geoid))
        else:
            print 'No update for:', geoid, location, pcode
            print old_lat, lat, old_lng, lng
    #
    # cleanup
    db.commit()
    c.close()
    
def main():
    # nothing here
    pass
    
if __name__ == "__main__":
    main()
