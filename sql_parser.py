import re
import os
#from collections import defaultdict 
import sys

class Statement:
    def __init__(self, file_name ,file_content):
        self.file_name = file_name
        self.file_content = self.formatScript (file_content)
        self.sttm_list = self.splitStatements (self.file_content)
        self.child_list,self.parent_list = self.childParentList(self.sttm_list)
        
    def formatScript (self,file_content):
        # WARNING fe_total_alert_list_mv has "';'" - solution to remove it
        file_content = file_content.lower().replace('"','').replace("';'","#")
        file_content = re.sub('/\*.*?\*/','',file_content, flags=re.DOTALL)
        file_content = re.sub('--.*?\n','',file_content)
        #remove prompt
        file_content = re.sub('prompt .*?\n','',file_content)
        return file_content
        
    def splitStatements(self,file_content):
        sttm_list = re.split(';|[ \t\n]+/[ \t\n]+',file_content)
        return (sttm_list)
        
    def childParentList (self,sttm_list):
        child_list = list()
        parent_list = list()
        for i in range(0,len(sttm_list)):
            curr_sttm = sttm_list[i]
            pattern_parse = re.compile('(alter|create\s+or\s+replace\s+force|create\s+or\s+replace|create)\s+'
            '(materialized\s+view|view|table|synonym)\s+(\w+)',flags=re.DOTALL)
            parsed_list = pattern_parse.findall(curr_sttm) # return [('alter', 'table'), ('create', 'view'), (foo, bar)]
            if parsed_list: #len(parsed_list) > 0:
                for y in range(0,len(parsed_list)):                
                    if ' '.join(parsed_list[y][1].split()) in ('table','view','materialized view'): #not really required
                        # find all FK 
                        pattern_ref = re.compile('.*?foreign\s+key.*?references\s*(\w+).*?',flags=re.DOTALL)
                        child_list.append(parsed_list[y][2])
                        parent_list = parent_list + pattern_ref.findall(curr_sttm) #return list of parent tables ['t1',t2]
                        #some tables created as "create as select"
                        if curr_sttm.count('select') > 0:
                            parent_list = parent_list + self.findFromSelect(curr_sttm)
                    #disabled
                    elif parsed_list[y][1] == 'synonym' and 1==2:
                        # find syn for
                        pattern_syn = re.compile('.*?for\s+(\w+).*?',flags=re.DOTALL)
                        child_list.append(parsed_list[y][2])
                        parent_list = parent_list + pattern_syn.findall(curr_sttm) 
                        
        child_list = list(set(child_list)) #unique
        parent_list = list(set(parent_list)) #unique
        return child_list, parent_list
        
    def findSubQuery(self,curr_sttm):
        stack = []
        curr_sttm = '(' + curr_sttm + ')'
        for char in curr_sttm:
            if char == '(':
                #stack push
                stack.append([])
            elif char == ')':
                yield ''.join(stack.pop())
            else:
                #stack peek
                stack[-1].append(char)
        return stack #return list()
        
    def findFromSelect (self,curr_sttm):
        tab = list()
        tokens = list()
        min_obj_len = 2
        #clean up
        curr_sttm = curr_sttm.replace(',',' , ').replace(')',' ) ').replace('(',' ( ').replace('"','')
        for x in self.findSubQuery (curr_sttm):
            #print('subquery: ',x)
            isfrom = False
            tokens = x.split()
            #adding one element in the end, because after "subquerying"
            #we can have query like
            #"select foo from"
            # from query with subquery
            # "select foo from (subquery)"
            # WARNING fe_total_alert_list_mv has "';'" - workaround to remove it above in replace
            tokens.append(';')
            maxlen = len(tokens)
            #print('maxlen',maxlen)
            for i in range(0,maxlen):
                #print('tokens: ',tokens)
                #print("i",i)
                if tokens[i] =='from' and i < maxlen and not isfrom:
                    #print('con1: ',tokens[i+1])
                    #print('len(tokens[i+1])',len(tokens[i+1])) 
                    
                    #1) query could be like "select foo, bar from where" because there 
                    #was subquery between "from" and "where" or "group":
                    #"select foo from (subquery) where"
                    #2)also suppose we have no objects with name len < min_obj_len
                    #so it's alias of subquery and not object name:
                    #select foo from (subquery) tbl where             
                    if tokens[i+1] not in('where','group') and len(tokens[i+1])> min_obj_len:
                        tab.append(tokens[i+1])            
                    isfrom = True
                if tokens[i] == ',' and i<maxlen and isfrom:
                    #print('con2: ',tokens[i+1])
                    if tokens[i+1] != '(': # case of subquery in () in from
                        tab.append(tokens[i+1])
                if isfrom and tokens[i] in ('where',';',')','(','union','order','group'):
                    #print('con3: ')
                    isfrom = False
                if tokens[i] == 'join' and i<maxlen:
                    #print('con4: ',tokens[i+1])
                    #suppose we have no objects with name len < min_obj_len
                    if len(tokens[i+1]) > min_obj_len:
                        tab.append(tokens[i+1])
        return tab   
        
    @property
    def showInfo(self):
        parent_str = str()
        parent_str = ', '.join(str(x) for x in self.parent_list)
        child_str = str()
        child_str = ', '.join(str(x) for x in self.child_list)
        #sttm_str = str()
        #sttm_str = ';\n '.join(str(x) for x in self.sttm_list)
        return ('\n' + '*'*25 +'\n'
        '*** Filename: %s\n'
        #'*** File content: %s\n'
        #'*** File statements: %s\n'
        '*** Object names: %s\n'
        '*** Object parents: %s ' % 
        (self.file_name,
        #self.file_content,
        #sttm_str,
        child_str,
        parent_str))

def whereCreated(dep_list,obj_name):
    where_list = list()
    for i in range(0,len(dep_list)):
        if obj_name in dep_list[i].child_list:
            where_list.append(dep_list[i].file_name)     
    return where_list  
    
#returns object dependency list    
def dependencyList(dep_list):   
    ret_list = list()
    depends_on = list()
    for i in range(0,len(dep_list)):
        for x in dep_list[i].parent_list:
            #depends_on = ', '.join(whereCreated(dep_list,x))
            depends_on = whereCreated(dep_list,x)
            if len(depends_on)>0:
                #ret = ret + 'file  '+ dep_list[i].file_name + '   depends on  ' + depends_on + '\n'
                sub_list = list()
                sub_list.append(dep_list[i].file_name)
                sub_list.append(whereCreated(dep_list,x))
                ret_list.append(sub_list)
    return ret_list
'''
# REFACTORED!!! backup of function
# returns object dependency list + write log - writing should be refactored
def dependencyFileList(dep_list, statment_obj, trg_path):
    content = str() #log file content
    #show objects info - just headed
    for x in statment_obj:
        content = content + x.showInfo
    #corrected list - without errors    
    new_list=list()
    content = content + '\n*** Dependency list:'
    for i in range(0,len(dep_list)):
        #print(dep_list[i][0])
        depends_on = ', '.join(dep_list[i][1])
        content = content  +'\n' + 'File  '+ dep_list[i][0] + '   Depends on  ' + depends_on
        if len(dep_list[i][1]) > 1:
            content = content  +'\n' + '*** ERROR: object from file  ' + dep_list[i][0] + ' uses object created in two or more scripts: ' + depends_on
            content = content  +'\n' + '*** ERROR: only first file will be used in graph: '+dep_list[i][1][0] 
            dep_list[i][1] = dep_list[i][1] #[0] 
        #convert [file,[file1][file2]] to [file,file1]
        new_list.append([dep_list[i][0],dep_list[i][1][0]])

        # write report
    f = open(trg_path + '/' + 'SQL_analyser.log', 'w')
    f.write(content)
    f.close
    return new_list
 '''   

#returns object dependency list + write log - writing should be refactored
def dependencyListCleanUp(dep_list):
    new_list = list()   # corrected list - without errors
    error_list = list() # list with errors
    for x,y in dep_list:
        # case when one object is created/altered in more then two files
        if len(y) > 1:
            error_str = ('Same object is created or altered in two or more files: '
                         + ', '.join(y)
                         + '\nFile  ' + x + ' depends on it.'  
                         + '\nOnly first file is considered in consolidated deployment script: ' + y[0]
                         + '\nAll other files are skipped: '  
                         + ', '.join(y[1:])
                         + '\nPlease correct files and restart process...')  
            error_list.append(error_str)
        #take firs one: convert [file,[file1][file2]] to [file,file1]
        new_list.append([x,y[0]])         
    return new_list, error_list

     
     
     
def logDependency(dep_list,error_list,statment_obj, trg_path):
    content = str() #log file content
    separator = '\n' + '*'*50
    
    # log objects info
    content =  "*** Object's dependency information:"
    for x in statment_obj:
        content = content + x.showInfo
        
    # log file dependencies
    content = content + separator + '\n*** File dependencies:'
    for x,y in dep_list:
        content = content  +'\n' + 'File  '+ x + '   Depends on file ' +  y
    
    # log errors in dependencies
    content = content + separator + '\n*** File dependency errors:\n'
    for n, x in enumerate(error_list,1):
        content = content + ('\n*** ERROR(' + str(n) + '): ' 
                            + x.replace('\n','\n*** ERROR('+ str(n) +'): ')
                            + '\n')
     
    # write report
    f = open(trg_path + '/' + 'SQL_analyser.log', 'w')
    f.write(content)
    f.close
    
    
    
def dependencyToDeployment(filename_list,dep_list):
    #return list:
    final = ['DUMMY']
    
    #add files without parents
    for i in filename_list:
        if i not in [x[0] for x in dep_list]:
            final.append(i)
            #print('NO PARENT',i)
    #self refererce to exclude(table1 FK to table1)
    for i in range(0,len(dep_list)):
        if dep_list[i][0]==dep_list[i][1]:
            #print('REMOVE', i,y)
            dep_list[i][1] = 'DUMMY'
    
    #magic loop
    while dep_list:
        #print('REMAINS', len(dep_list))
        ch,par = dep_list.pop()
        
        #print('POP', ch,par)
        #which parents has this file:
        list_of_par = list()
        list_of_par.append(par)
        for ch1,par1 in dep_list:
            #print('SEARCH', ch, 'FOUND',ch1)        
            if ch == ch1:
                #cnt=+1
                #print('COUNT', cnt)
                list_of_par.append(par1)
        #print('FINAL COUNT',final.count(par))
        #print('ALL PARENTS ARE',' ,'.join(list_of_par))
        #if final.count(par) >= cnt or par == 'DUMMY':
        if set(final) >= set(list_of_par):
            if final.count(ch) == 0:
                final.append(ch)
                #print('APPEND', ch)
        else:
            #print('REVERT', ch)
            dep_list.insert(0,[ch,par])
    final.remove ('DUMMY')       
    return final

def writeDeploymentScript(final, path,file_name,src_path):
    content = str()
    content = content + '/**\tDeployment script: **/\n'
    for i in final:
        content = content + '\n@' + str(src_path) + '/'+ i
    # write deployment script     
    f = open(path  + '/' +  file_name, 'w')
    f.write(content)
    f.close

def printProgress (file_name,current_cnt,total_cnt):
    divider = total_cnt/25 #25 is total len of bar
    brogress_bar_line = ('Progress: %s%s %i / %i files    \r'  % 
        ( chr(9608)*int(current_cnt/divider), chr(9617)*int((total_cnt-current_cnt)/divider), current_cnt, total_cnt))
    loading_file_line = 'Loading file: ' + str(file_name) 
    #print('Loading file:', file_name + ' '*30)    
    print (loading_file_line + ' '*(len(brogress_bar_line) - len(loading_file_line)))
    print (brogress_bar_line,end='\r')
    '''
    print('Progress: [%s%s] %i / %i files    \r'  % 
         ( '#'*int(current_cnt/10), '.'*int(total_cnt/10-current_cnt/10), current_cnt, total_cnt),end='\r')
    '''
    
      

def main():    
    src_path = str()
    #trg_path = 'M:/Work/python'
    #list of "Statement" objects:
    statment_obj = list()
    
    
    #ask for anlt path
    while True:
        src_path = input('Enter path to anlt folder: ')
        if not os.path.isdir(src_path):
            print('Directory does not exist...\n')
        else:
            break
            
    src_path = src_path.replace("\\","/")
    # output dir is same as input anlt dir - could be changed:
    trg_path = src_path 
    
    # scan CMO or FMO path for grants?
    # actually this CMO and FMO is not required in svn - grants are same
    while True:
        cmo_fmo  = input('CMO or FMO svn structure: ').upper()
        if cmo_fmo not in ('CMO','FMO'):
            print('CMO and FMO are only possible values...\n')
        else:
            break
          
    #list of directories and also sequence of reading
    dirs = ('sequence','synonym','table','view','materialized_view','function','package','trigger','grant/' + cmo_fmo,'data_load_scripts')
    
    #there are four lists with filenames:
    filename_list  = list()     # all filenames which are read from folders are here
    deployment_top = list()     # filelist of objects which are deployed firstly, e.g. sequences, synonyms
    deployment_mid = list()     # filelist of objects which are deployed, here we will have objects with dependencies
    deployment_bottom = list()  # filelist of objects which are deployed in the end, e.g. grants
    
    #count total files - just for UI
    total_cnt = 0
    for subdir in dirs:
        try:
            for x in os.listdir(src_path + '/'+ subdir ):
                total_cnt +=1
        except IOError:
            continue
    current_cnt = 1   #just for count in progress for UI     
    
    # run through directories
    # and depending on folder (object type) use different behaviour
    for subdir in dirs: 
        try:
            files_list = os.listdir(src_path + '/'+ subdir ) #for frogress bar
            for x in files_list:
                printProgress ('../' +  subdir + '/' + x, current_cnt, total_cnt)
                # for tables, views, mviews the order is important
                if subdir in ('table','view','materialized_view'):
                    f = open(src_path + '/' + subdir + '/' + x, 'r')
                    content = f.read()
                    f.close
                    filename_list.append (subdir + '/' + x)
                    curr_statement = Statement(subdir + '/' + x, content)
                    statment_obj.append(curr_statement)           
                # sequences and syns are always on top
                elif subdir in ('sequence','synonym'):
                    deployment_top.append(subdir + '/' + x)
                # grants, and pl/sql always in the end (bottom list)    
                elif subdir in ('grant' + cmo_fmo,'function','package','trigger'):
                    deployment_bottom.append(subdir + '/' + x)    
                # dataload scripts are in the end
                # alter_session_disable_parallel_dml - is workaround to have smooth 
                # dml deployment - should be included to the scripts
                elif subdir in ('data_load_scripts'):
                    deployment_bottom.append('alter_session_disable_parallel_dml.sql')
                    deployment_bottom.append(subdir + '/' + x) 
                current_cnt +=1 #increment just for count in progress bar
        except IOError:
            print('Warning: folder "' + subdir + '" does not exist and skiped' + ' ' * 30 + '\r')
            continue    
        
        
    print('\nProcessing: find dependencies ... ')        
    dep_list = dependencyList(statment_obj)
    
    """
    print('Processing: find file dependencies ... ')  
    dep_list = dependencyFileList(dep_list,statment_obj,trg_path)
    """
    
    print('Processing: cleaning up dependencies ... ')  
    dep_list, error_list = dependencyListCleanUp(dep_list)
    
    #list with errors
    print('Processing: write log-file ... ')  
    logDependency(dep_list,error_list,statment_obj, trg_path)
    
    
    
    
    print('Processing: building deployment script ... ')
    deployment_mid = dependencyToDeployment(filename_list,dep_list)
    
    print('Processing: saving deployment script ... ')
    writeDeploymentScript(deployment_top + deployment_mid + deployment_bottom,trg_path,'SQL_analyser.sql',src_path)
    #writeDeploymentScript(deployment_top + deployment_mid + deployment_bottom,trg_path,'SQL_analyser.sql','')
    
    print('Done!\nReport file:',trg_path + '/' + 'SQL_analyser.log')
    print('Deployment file:',trg_path + '/' + 'SQL_analyser.sql')
    if len(error_list) > 0:
        print('There are errors: please check log file')
    input('Press enter key to exit')
    
if __name__ == '__main__':
    # wrapping for user keyboard interrupt
    try:
        main()
    except KeyboardInterrupt:
        print('\nExit by user interrupt...')    
    
    
    
    
    
