import re
import os
from collections import defaultdict 
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
        return ('\n***************************\n'
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
    
#returns dependency list    
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

#not used - to be deleted
def enreachList(dep_list,file_list):
    append_list = list()
    for i in file_list:
        for y in range(0,len(dep_list)):
            if dep_list[y][0] == i:
                break
        else:
            append_list = [i,[i]]
            #print(append_list)
            #dep_list. = dep_list + append_list
            dep_list.append (append_list)
    return dep_list

#not used - to be deleted    
#add missed roots like '['root_file','root_file']'    
def enreachList1(dep_list):
    for fch,fpar in dep_list:
        #print('fch,fpar',fch,fpar)
        if fpar not in [x[0] for x in dep_list]:
            #print('not in')
            dep_list.append([fpar,fpar])      
    return dep_list

#map nodel without parents to 'root'
#add root-root as superroot
#remove self fks    
#not used - to be deleted
def enreachList2(dep_list):
    for fch,fpar in dep_list:
        #print('fch,fpar',fch,fpar)
        if fpar not in [x[0] for x in dep_list]:
            #print('not in')
            dep_list.append([fpar,'root'])
        if fch == fpar:
            dep_list.remove([fch,fpar])
    dep_list.append(['root','root'])    
    return dep_list

    
def displayGraph(id, nodes, level,ret_str): 
        #print('%s%s%s level: %s' % (' ' * level, '\\__', id,level)) 
    ret_str = ret_str + str(' ' * level) + '\\__' + str(id)+ ' Level: ' + str(level) +'\n'
    for child in sorted(nodes.get(id, [])): 
        ret_str = displayGraph(child, nodes, level + 1,ret_str) 
    return ret_str        
               
def orderList(id, nodes, level,ret_list): 
        #print('%s%s%s level: %s' % (' ' * level, '\\__', id,level)) 
    ret_list.append(str(level) +': '+ str(id))
    for child in sorted(nodes.get(id, [])): 
        ret_list = orderList(child, nodes, level + 1,ret_list) 
    return ret_list        

def orderList1(id, nodes, level,ret_list): 
        #print('%s%s%s level: %s' % (' ' * level, '\\__', id,level)) 
    ret_list.append([level,str(id)])
    for child in sorted(nodes.get(id, [])): 
        ret_list = orderList1(child, nodes, level + 1,ret_list) 
    return ret_list  
    
def progressbar(it, prefix = "", size = 60):
    count = len(it)
    def _show(_i):
        x = int(size*_i/count)
        sys.stdout.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), _i, count))
        sys.stdout.flush()
    
    _show(0)
    for i, item in enumerate(it):   
        yield item
        _show(i+1)
    sys.stdout.write("\n")
    sys.stdout.flush()

  
if __name__ == '__main__':
    src_path = str()
    trg_path = 'M:/Work/python'
    content = str()
    statment_obj = list()
    
    src_path =input('Enter path to anlt folder: ')
    src_path = src_path.replace("\\","/")

    
    #dirs = ('table','view','materialized_view','synonym')
    dirs = ('table','view','materialized_view')
    filename_list = list()
    
    #count total files
    total_cnt = 0
    for subdir in dirs:
        for x in os.listdir(src_path + '/'+ subdir ):
            total_cnt +=1
    current_cnt = 1   #just for count in progress     
        
    for subdir in dirs: 
        #files = os.listdir(src_path + '/'+ subdir ) #for frogress bar
        for x in os.listdir(src_path + '/'+ subdir ):
            print('Loading file:', src_path + '/'+ subdir +'/' + x + ' '*20)
            print('Progress: [%s%s] %i / %i files    \r'  % 
                ( '#'*int(current_cnt/10), '.'*int(total_cnt/10-current_cnt/10), current_cnt, total_cnt),end='\r')
            #sys.stdout.write('Load file:'+ src_path + '/'+ subdir +'/' + x +'\r')
            #sys.stdout.flush()
            f = open(src_path + '/' + subdir + '/' + x, 'r')
            content = f.read()
            f.close
            filename_list.append (subdir + '/' + x)
            curr_statement = Statement(subdir + '/' + x,content)
            statment_obj.append(curr_statement)
            current_cnt +=1 #just for count in progress
    content = str()  
    print('\nProcessing ... ')
    
    #show objects info
    for x in statment_obj:
        content = content + x.showInfo
        
    #find  file dependency    
    dep_list = dependencyList(statment_obj)
    
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
    dep_list = new_list

    
    
    '''
    dep_list = enreachList2(dep_list)
    
    
    for a,b in dep_list:
        print(a,b)
    
    
    #content = content + 'start\n'
    
    #page_ids = [ ('file1', 'file1'),('file2', 'file1'),('file3', 'file1'),
    #            ('file4', 'file2'), ('file5', 'file4'),('file6', 'file3'),
    #            ('fileX', 'file7')] 
             
    
    page_ids = list() 
    small_tuple = tuple()
    #page_ids.append( ('file1', 'file1'))
    #print(page_ids)
    for i in range(0,len(dep_list)):
        #small_tuple = (dep_list[i][0],''.join(dep_list[i][1]))
        small_tuple = (dep_list[i][0], dep_list[i][1])
        #small_tuple = (''.join(dep_list[i][1]),dep_list[i][0])
        page_ids.append(small_tuple)
    ##page_ids.append(('view/a.sql', 'view/a.sql'))
    ##page_ids.append(('view/q.sql', 'view/q.sql'))
       
    
    nodes, roots = defaultdict(set), set() 

    for article, parent in page_ids: 
        if article == parent: 
            roots.add(article) 
        else: 
            nodes[parent].add(article)
    
    content = content + '\n***** GRAPH*****\nstart\n'
    
    union_list = list()
        
    for id in sorted(roots): 
        content = content + displayGraph(id, nodes, 0,'') 
        #union_list = union_list + orderList(id, nodes, 0,list()) 
        union_list = orderList1(id, nodes, 0,union_list) 
        
    #print(union_list)
    
    #16.09.2017 test logic
    temp_list = list()
    max_level = 0
    for level,file in union_list:
        max_level = level
        for l,f in union_list:
            if file == f and level<l:
                max_level = l
        if file not in [x[1] for x in temp_list]:
            temp_list.append([max_level,file])
    union_list =  temp_list       
    #16.09.2017 end of test logic
    
    max_level = 0
    for level,file in union_list:
        if level > max_level:
            max_level = level
    temp_list = list()
    
    for l in range(0,max_level+1):
        for level,file in union_list:
            if level == l:
                print(l,file)
                temp_list.append(file)
        
    union_list = temp_list

    
    for i in filename_list:
        if i not in union_list:
            union_list.insert(0,i)
        
    final_list = list()
    for i in union_list:
        if i not in final_list:
            final_list.append(i)
    
    for i in final_list:
        content = content + '\n@' + src_path + '/' +i
        
    '''    
    #completely new approach
    
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
            
    
    #completely new approach end 
    
    # write report
    f = open(trg_path + '/' + 'SQL_analyser.log', 'w')
    f.write(content)
    f.close
    
    content = str()
    final.remove ('DUMMY')
    content = content + '/**\tDeployment script: **/\n'
    for i in final:
        content = content + '\n@' + src_path + '/' + i
    
    # write deployment script     
    f = open(trg_path + '/' + 'SQL_analyser.sql', 'w')
    f.write(content)
    f.close
    
    print('Done!\nReport file:',trg_path + '/' + 'SQL_analyser.log')
    print('Deployment file:',trg_path + '/' + 'SQL_analyser.sql')
    input('Press any key to exit')
    
    
    
    
    
    
    