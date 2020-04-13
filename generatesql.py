from typing import TextIO
import pandas as pd
import sys
import xlrd
import os
import subprocess
from datetime import datetime
import logging
import time

def format_query(total_sql, gp_cols,table_name, where_col,source_col,grp,input_list):
    where_find = 0
    group_find = 0
    have_find  = 0
    group_col = []
    grp_col=[]
    temp_wh=''
    from_find = 0
    total = ''
    wh_col =[]
    where_cols = []
    is_where = 0
    gp = 0
    num = 0
    case_col=''
    for i in range(len(where_col)):
        if (where_col[i]) == 'NA':
            continue;
        else:
            is_where = 1
            wh_col = where_col[i].split(" or ")
            for j in range(len(wh_col)):
                if j < len(wh_col)-1:
                    temp_wh = wh_col[j].split(" ")
                    for n in range(len(temp_wh)):
                        if temp_wh[n].upper()=='CASE':
                            total,case_col,group = case_when(input_list[i],wh_col[j])  
                            total=table_name[i].split(" ")[1]+"."+source_col[0][i]+" = "+total[7:total.find(" AS")]+'OR '
                            break;

                        else:
                            if temp_wh[n].find('BETWEEN') != -1:
                                total = total + table_name[i].split(" ")[1]+"."+source_col[0][i]+" "+wh_col[j]+" OR "
                                break;
                        total = total + table_name[i].split(" ")[1]+"."+source_col[0][i]+" "+wh_col[j]+" OR "
                else:
                    temp_wh = wh_col[j].split(" ")
                    if temp_wh[0].upper()== 'CASE':
                        total,case_col,group = case_when(input_list[i],wh_col[j])
                        total= table_name[i].split(" ")[1]+"."+source_col[0][i]+" = "+total[7:total.find(" AS")]+'AND\n'
                        break;
                    elif temp_wh[0].upper() in string_functions:
                        total,case_col,group = string_opr(input_list[i],wh_col[j],wh_col[j])
                        total= table_name[i].split(" ")[1]+"."+source_col[0][i]+" = "+total+'AND\n'
                        break;
                    else:
                        total = total+ table_name[i].split(" ")[1]+"."+source_col[0][i]+" "+wh_col[len(wh_col)-1]+" AND\n"
    if total:
            total = total[:-4] 
            where_cols.append(total)

    for i in range(len(gp_cols)):
        if gp_cols[i].isdigit()==0:
            if grp[i] == 1:
                gp = 1
                num = num + 1
                group_col.append(gp_cols[i])
            else:
                gp = 2
                grp_col.append(gp_cols[i])
    formatted_sql.append('Select')
    for items in total_sql:
        
        if type(items) is float or items == '':
            continue;
        else:
            if items.find("from ") != -1:
                select_column = items[items.find("Select")+6:items.find("from ")]
            else:
                select_column = items[items.find("Select")+6:]
                from_find = 1;
        select_columns = []
        if type(select_column) is float or select_column == '':
            continue;
        else:
            select_columns = select_column
        formatted_sql.append(select_columns+",")
        file_out.write(select_columns + ",\n")
    if(from_find == 0):
        formatted_sql.append('from')
        formatted_sql.append(base_table)
        for items in total_sql:
             if items.find("JOIN") == -1:
                    if items.find(" group by ") == -1:
                        from_columns = items[items.find(" from ")+6:]
                        if(from_columns != ''):
                            
                            file_out.write(from_columns+"\n")
                            formatted_sql.append(from_columns)
                    else:
                        from_columns = items[items.find(" from ")+6:items.find(" group by ")]
                        if(from_columns != ''):
                            file_out.write(from_columns+"\n")
                            formatted_sql.append(from_columns)
             else:
                 join_table = items[items.find(" from ")+6:items.find(" ON")+3]
                 file_out.write(join_table + "\n")
                 if items.find(" where ") == -1:
                     if items.find(" group by ") == -1:
                         join_columns = items[items.find(" ON")+3:]
                     else:
                         join_columns = items[items.find("ON")+3:items.find(" group by ")]
                 else:
                     join_columns = items[items.find("ON")+3:items.find("where")]
                     where_find = 1
                 file_out.write(join_columns + "\n")
                 if join_columns != '':
                     formatted_sql.append(join_table+" "+join_columns)  
                 
        if (where_find != 0 or is_where==1):
            formatted_sql.append("where ")
            
        for items in total_sql:
            if items.find(" where ") != -1:
                if items.find(" group by ") != -1:
                    where_columns = items[items.find(" where ")+7:items.find(" group by ")]
                    group_find = 1
                else:
                    where_columns = items[items.find(" where ")+7:]
                file_out.write(where_columns +",\n")
                formatted_sql.append("("+where_columns +") OR")
            elif items.find(" group by") != -1:
                group_find = 2  
        if (where_cols):
            for element1 in where_cols:
                for i in range(len(where_cols)):
                    formatted_sql.append(element1+' OR')
                    file_out.write(element1 +" OR\n")
        if group_find != 0 and num != len(gp_cols):
            formatted_sql.append("group by")    
        for items in total_sql:      
            if items.find(" group by ") != -1:
                if items.find(" having ") != -1:
                    group_columns = items[items.find(" group by ")+10:items.find("having ")-1]
                    have_find = 1
                    file_out.write(group_columns +",\n")
                    formatted_sql.append(group_columns+",")
                else:
                    for items in total_sql:      
                        if items.find(" group by ") != -1:
                            if items.find(" having ") != -1:
                                group_columns = items[items.find(" group by ")+10:items.find("having ")-1]
                                file_out.write(group_columns +",\n")
                                formatted_sql.append(group_columns+",")
                                have_find = 1
                            else:
                                group_columns = items[items.find(" group by ")+10:]
                                if(group_columns):
                                    file_out.write(group_columns +",\n")
                                    formatted_sql.append(group_columns+",")
                
                if gp == 1 and num == len(grp):
                    continue
                else:
                    for element in grp_col:
                        element1 = element.split(",")
                        try:
                            element1.remove(' ')
                        except:
                            try:
                                element1.remove('')
                            except:
                                element1 = element1
                        for i in range(len(element1)):
                            if element1[i] != 'f1':
                                formatted_sql.append(element1[i]+',')
                                file_out.write(element1[i] +",\n")
                            
        if have_find != 0:
            formatted_sql.append("having")
        for items in total_sql:
            if items.find(" having ") != -1:
                having_columns = items[items.find(" having ")+8:]
                file_out.write(having_columns +",\n")
                formatted_sql.append("("+having_columns+") OR")
    formatted_sql.append(';')
    logging.debug("formatted sql: ")
	logging.debug(formatted_sql)

def derived(input_list,row,val):
    rule_break = input_list['TRANSFORMATION'].split()
    var =''
    sl = 0
    val1= ''
    for items in rule_break:
        file_out.write(items.upper())
        if items.upper() in string_functions:
            if val.find("group by") == -1:
                    val1 = val
            else:
                    sl = val.index("group")
                    val1 = ''.join(val[:sl-1])
            derived_port , gp_cols ,group= string_opr(input_list,row,val1)
            group = 0
            if (val.find("where") == -1 and val.find("group by") == -1):
                logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE'])
                return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE'] , gp_cols,group
            elif val.find("group by") != -1:
                     b = val.index("group by")
                     try:
                         h = val.index("having")
                         having_break = (''.join(val[h+7:]))
                         abc = having_break.split(" ")
                         if abc[0] in aggregate_list:
                             derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                             having_cond = ' '.join(abc[2:])
                             return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+ derived_portt +having_cond , gp_cols  ,group
                         else:
                             c1 = val.index("(")
                             logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                             return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:] , gp_cols  ,group          
                     except:
                         logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                         return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:] , gp_cols ,group         

        elif items in maths_list:
            derived_port , gp_cols,group = maths_opr(input_list,row)
            group = 0
            for i in col:
                var=var+input_list['SOURCE_TABLE'].split(" ")[1]+"."+i+",\n"  
            if (val.find("where") == -1 and val.find("group by") == -1):
                logging.debug(' -- '+items.upper()+' -- '+"Select "+derived_port+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE'])
                return "Select "+derived_port+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE'], gp_cols   ,group    
            elif val.find("group by") == -1:
                b = val.index("group by")
                try:
                   h = val.index("having")
                   c1 = val.index("(")
                   having_break = (''.join(val[h+7:]))
                   abc = having_break.split(" ")
                   if abc[0] in aggregate_list:
                       derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                       having_cond = ' '.join(abc[2:])
                       return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+ derived_portt +having_cond , gp_cols  ,group
                   else:
                       logging.debug(' -- '+items.upper()+' -- '+"Select "+derived_port+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                       return "Select "+derived_port+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:], gp_cols,group

                except:
                    logging.debug(' -- '+items.upper()+' -- '+"Select "+derived_port+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                    return "Select "+derived_port+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:] , gp_cols,group
                
        elif items.upper() in date_list:
            derived_port , gp_cols,group = date_func(input_list)
            if (val.find("where") == -1 and val.find("group by") == -1):
                logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE'])
                return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE'] , gp_cols,group
            elif val.find("group by") == -1:
                     b = val.index("group by")
                     try:
                         h = val.index("having")
                         c1 = val.index("(")
                         having_break = (''.join(val[h+7:]))
                         abc = having_break.split(" ")
                         if abc[0] in aggregate_list:
                             derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                             having_cond = ' '.join(abc[2:])
                             return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+ derived_portt +having_cond , gp_cols  ,group
                         else:
                             logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                             return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:] , gp_cols  ,group          
                     except:
                         logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                         return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:] , gp_cols ,group         
        
        elif items.upper() in aggregate_list:
            derived_port , gp_cols ,group = aggregate_fun(input_list,val)
            group = 1
            gp_cols = 'f1'
            if (val.find("where") == -1 and val.find("group by") == -1):
                logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE'] +" group by ")
                return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by " , gp_cols,group
            elif val.find("group by") != -1:
                c = val.index("group by")
                if ( val.find("having") != -1):
                    h = val.index("having")
                    c1 = val.index("(") 
                    logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:h-1]+\
                         " having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                    return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:h-1]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:] ,gp_cols,group
                else:
                    logging.debug(' -- '+items.upper()+' -- '+"Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:])
                    return "Select " + derived_port + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:],gp_cols,group
        
        
        elif items.upper() in lists:
            if val.find("group by") == -1:
                    val1 = val
            else:
                    sl = val.index("group")
                    val1 = ''.join(val[:sl-1])
            derived_port , gp_cols ,group= case_when(input_list,val1)
            group = 0
            logging.debug(' -- '+items.upper()+' -- '+derived_port)
            if (val.find("where") == -1 and val.find("group by") == -1):
                logging.debug(' -- '+items.upper()+' -- '+derived_port)
                #logging.info(' -- '+items.upper()+' -- '+'EXIT the function')
                return derived_port , gp_cols,group
            elif val.find("group by") != -1:
                c = val.index("group by")
                if ( val.find("having") != -1):
                    h = val.index("having")
                    having_break = (''.join(val[h+7:]))
                    abc = having_break.split(" ")
                    if abc[0] in aggregate_list:
                        derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                        having_cond = ' '.join(abc[2:])
                        return derived_port+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h-1]+" having "+ derived_portt +having_cond , gp_cols  ,group
                    else:
                        c1 = val.index("(") 
                        logging.debug(' -- '+items.upper()+' -- '+derived_port+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:h-1]+\
                             " having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                        return derived_port+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:h-1]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:] ,gp_cols,group
                else:
                    logging.debug(' -- '+items.upper()+' -- '+derived_port+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:])
                    return derived_port+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c+9:],gp_cols,group
            
        elif items.upper() in ranking:
            derived_port , gp_cols ,group = row_number(input_list,val)
            logging.debug(' -- '+items.upper()+' -- '+derived_port)
            return derived_port , gp_cols,group
        
        elif items.upper() == "DISTINCT":
            group = 0
            if len(rule_break)==1:
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']
                logging.debug(' -- '+ items.upper()+' -- '+"Select "+ input_list['TRANSFORMATION'] +" "+input_list['SOURCE_COLUMN'] + " from "+input_list['SOURCE_TABLE'])
                return  "Select "+ input_list['TRANSFORMATION'] +" "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN'] +" AS "+ input_list['TARGET_COLUMN']+ " from "+input_list['SOURCE_TABLE'], gp_cols, group
            else:
                for i in range(1,len(rule_break)):
                    if rule_break[i] in string_functions:
                        val1 = ' '.join(rule_break[i:])
                        derived_port , gp_cols ,group= string_opr(input_list,row,val1)
                        group = 0
                    elif rule_break[i] in maths_list:
                        val1 = ' '.join(rule_break[1:])
                        derived_port , gp_cols ,group= maths_opr(input_list,val1)
                        group = 0                    
                return  "Select DISTINCT "+derived_port+" AS "+ input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE'], gp_cols, group


def date_func(input_list):
    group = 0
    func_break = input_list['TRANSFORMATION'].split()
    if func_break[0].upper() == 'CURRENTDATE':
        gp_cols = 'f1'
        logging.debug(' -- '+func_break[0].upper() + ' -- '+"current_date" )
        return "current_date", str(gp_cols),group
    elif func_break[0].upper() == 'CURRENTTIMESTAMP':
        gp_cols = 'f1'
        logging.debug(' -- '+func_break[0].upper() + ' -- '+ "current_timestamp")
        return "current_timestamp", str(gp_cols),group
    elif func_break[0].upper() == 'DATEDIFFERENCE':
        if len(func_break) >=3:
            if val.find("current_date") != -1 :
                if func_break[2] == "current_date":
                    logging.debug(' -- '+func_break[0].upper() + ' -- '+"datediff("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+","+func_break[2]+")" )
                    return "datediff("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+","+func_break[2]+")", input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group
                elif func_break[1] == "current_date":
                    logging.debug(' -- '+func_break[0].upper() + ' -- '+ "datediff("+func_break[1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")")
                    return "datediff("+func_break[1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")" ,input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2],group
            else:
                logging.debug(' -- '+func_break[0].upper() + ' -- '+"datediff("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")")
                return "datediff("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")" , input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2],group
    elif func_break[0].upper() == 'DATEFORMAT':
        if len(func_break) >= 4:
            logging.debug(' -- '+"from_unixtime(unix_timestamp("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+",'"+func_break[2]+"'),'"+func_break[3]+"')")
            return "from_unixtime(unix_timestamp("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+",'"+func_break[2]+"'),'"+func_break[3]+"')" , input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group
    elif func_break[0].upper() == 'DATEADD':
        logging.debug(' -- '+func_break[0].upper() + ' -- '+ "date_add("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+func_break[2]+")")
        return "date_add("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+func_break[2]+")",input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group
    elif func_break[0].upper() == 'DATESUB':
        logging.debug(' -- '+func_break[0].upper() + ' -- '+ "date_sub("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+func_break[2]+")")
        return "date_sub("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+func_break[2]+")",input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group
    elif func_break[0].upper() == 'MONTHSBET':
            if val.find("current_date") != -1 :
                if func_break[2] == "current_date":
                    logging.debug(' -- '+func_break[0].upper() + ' -- '+ "months_between("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+","+func_break[2]+")")
                    return "months_between("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+","+func_break[2]+")", input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group
                elif func_break[1] == "current_date":
                    logging.debug(' -- '+func_break[0].upper() + ' -- '+ "months_between("+func_break[1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")")
                    return "months_between("+func_break[1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")" ,input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2],group
            else:
                logging.debug(' -- '+func_break[0].upper() + ' -- '+"months_between("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")")
                return "months_between("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+", "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2]+")", input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[2],group
    elif func_break[0].upper() == 'TODATE':
        logging.debug(' -- '+func_break[0].upper() + ' -- '+ "to_date("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+")")
        return "to_date("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+")",input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group
    elif func_break[0].upper() == 'EXTYEAR':
        logging.debug(' -- '+ func_break[0].upper() + ' -- '+ "year("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+")" )
        return "year("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+")",input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group
    elif func_break[0].upper() == 'EXTMONTH':
        logging.debug(' -- '+func_break[0].upper() + ' -- '+ "month("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+")" )
        return "month("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1]+")",input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[1],group

        
def maths_opr(input_list,row):
    group = 0
    func_split = row.split()
    func = []
    global flag1 
    global post_oprnd
    global pre_oprnd
    temp_cols = ''
    gp_cols =''
    gp_col = ''
    for item in func_split:
        if ( item == "group"):
            logging.debug(' -- ' + item + ' -- math operations')
            break
        elif item in maths_list:
            logging.debug(' -- ' + item + ' -- math operations')
            func.append(item)
        else:
            if  item.upper() not in string_functions and item.isdigit() == 0:
                temp_cols = temp_cols+input_list['SOURCE_TABLE'].split(" ")[1]+"."+ item +","
                logging.debug(' -- '+ temp_cols +' -- math operands')
                gp_cols = temp_cols[:-1]
    lt = len(func)
    v = ""
    count = 0
    count1 = 0
    sp = 0
    flag1 = 0
    last_oprnd = []
    for item in func:
        if lt > 1:
            if func.index(item) == 0 and count1 == 0:
                sp = func_split.index(item)
                count1 += 1
            else:
                sp = func_split.index(item,sp+1)
                count1 += 1
        else:
            sp = func_split.index(item)
        if count == 0:
            f2 = func_split[:sp]
            post_oprnd , flag1 = nesting_func(input_list,row,val,f2,flag1)
            sp1 = sp
            last_oprnd = func_split[sp+1:]
            count  += 1
        else:
            f2 = func_split[sp1+1:sp]
            post_oprnd, flag1 = nesting_func(input_list,row,val,f2,flag1)
            sp1 = sp
            last_oprnd = func_split[sp+1:]
            
        if (flag1)> 0:
            v += ''.join(post_oprnd)+ func_split[sp]
            flag1 = 0
        else:
            if len(func_split) > 2:
                if ( len(func_split) == 2) or len(f2) == 1:
                    if func_split[sp - 1].isdigit() == 0:
                        v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_split[sp - 1] + func_split[sp]
                        gp_col = gp_col +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_split[sp - 1]
                    else:
                        v += func_split[sp - 1] + func_split[sp]
                else:
                    v += func_split[sp]
            else:
                v += func_split[sp]
    if val.find("group") != -1:
        if (flag1) > 0:
            tot = v +''.join(post_oprnd)
            flag1 = 0
        else:
            tot = v + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_split[func_split.index("group")- 1]
            gp_col = gp_col + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_split[func_split.index("group")- 1]
    else:
        if (flag1) > 0:
            tot = v +''.join(post_oprnd)
            flag1 = 0
        else: 
            if func_split[-1].isdigit() == 0:
                for item in last_oprnd:
                    if item.upper() in string_functions:
                       post_oprnd, flag1 = nesting_func(input_list,row,val,last_oprnd,flag1)
                       tot = v +''.join(post_oprnd)
                       
                       break
                    else:
                        tot = v + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_split[-1]
                        gp_col = gp_col + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_split[-1]
                        break
            else:
                for item in last_oprnd:
                    if item.upper() in string_functions:
                       post_oprnd, flag1 = nesting_func(input_list,row,val,last_oprnd,flag1)
                       tot = v +''.join(post_oprnd)
                       
                       break
                    else:
                        
                        tot = v + func_split[-1]
                    break
    logging.debug(' -- '+ tot +' -- math expression')
    return tot , gp_cols,group

def nesting_func(input_list,row,val,f2,flag1):
    sub_fun = ''
    post_oprnd = []
    for items in f2:
        if items.upper() in string_functions:
            i = f2.index(items)
            if val.find("group") == -1:
                sub_func = f2[i:]
            else:
                g1 = f2.index("group")
                sub_func = f2[i:g1]
            
            for i in range(len(sub_func)):
                sub_fun = sub_fun +sub_func[i]+" "
            derived_port , gp_cols ,group= string_opr(input_list,sub_fun,sub_fun)
            flag1 = 1
            post_oprnd = derived_port.split()
            logging.debug(' -- '+derived_port )
            return post_oprnd,flag1
    return post_oprnd,flag1


def string_opr(input_list,row,val):
    group = 0 
    func = row.split()
    func_break = val.split()
    var =''
    cols = ''
    for items in func:
        if items.upper() == 'CONCAT':
            group = 2
            gp_col = ''
            t1 = ''
            tot = ''
            col1 = []
            gp_cols = ''
            l1 = func_break.index('CONCAT')
            v = '' 
            i = 1;
            while i <= len(func_break)-1:
                var = ''
                if func_break[i] in string_functions:
                    t = ' '.join(func_break[i:])
                    t1 , gp_cols , group = string_opr(input_list, t , t)
                    cols = cols +gp_cols+","
                    func_break = t1.split(" ")
                    if len(func_break) == 1:
                        v = v + func_break[0]+","
                        break
                    elif len(func_break) > 1:
                        for n in range(0 , len(func_break)):
                            if var == 'up':
                                i = -group
                                break;
                            if n == 0:
                                temp = func_break[0]+","
                                i = i+ group
                            elif func_break[n] not in string_functions:
                                if func_break[n].isdigit()==1:
                                    temp = temp +func_break[n] +","
                                else:
                                    if func_break[n].find("\"") != -1:
                                        temp = temp + func_break[n] +","
                                    elif func_break[n].find("\'") != -1:
                                        temp = temp + func_break[n] +","
                                    else:
                                        temp = temp + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n] +","
                                        cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n]+","
                            else:
                                var = ' '.join(func_break[n:])
                                func_break = var.split()
                                var = 'up'
                                v = v + temp
                                temp = ''
                                #break;
                                continue;
                        v = v+ temp
                else:
                    group = 1
                    if func_break[i].isdigit()==1:
                        v += func_break[i]+","
                    else:
                        if func_break[i].find("\"") != -1:
                            v += func_break[i]+","
                        elif func_break[i].find("''") != -1:
                            v += "\" \","
                        elif func_break[i].find("\'") != -1:
                            v += func_break[i]+","
                        else:
                            v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                            cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                i = i + group
            tot = v[:-1]
            gp_cols =cols[:-1]
            logging.debug(' -- '+ items.upper()+' -- '+tot)
            return "CONCAT("+tot +")" , gp_cols,group
                
        elif items.upper() == 'SUBSTR' or items.upper() == 'LOCATE':
            group = 4
            string = 0
            t1 = ''
            tot = ''
            col1 = []
            var = ''
            v = '' 
            i = 1;
            while i <= len(func_break)-1:
                var = ''
                if func_break[i] in string_functions:
                    t = ' '.join(func_break[i:])
                    t1 , gp_cols , group = string_opr(input_list, t , t)
                    cols = cols+gp_cols+","
                    func_break = t1.split(" ")
                    if len(func_break) == 1:
                        v = v + func_break[0]+","
                        break
                        
                    elif len(func_break) > 1:
                        for n in range(0 , len(func_break)):
                            if var == 'up':
                                i = -group
                                
                                continue;
                                #break;
                            if n == 0:
                                temp = func_break[0]+","
                                i = i+ group
                                continue
                            if func_break[n] not in string_functions:
                                if func_break[n].isdigit()==1:
                                    temp = temp +func_break[n] +","
                                else:
                                    if func_break[n].find("\"") != -1:
                                        temp = temp + func_break[n] +","
                                    elif func_break[n].find("\'") != -1:
                                        temp = temp + func_break[n] +","
                                    else:
                                        temp = temp + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n] +","
                                        cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n]+","
                            else:
                                var = ' '.join(func_break[n:])
                                func_break = var.split()
                                var = 'up'
                                v = v + temp
                                temp = ''
                                continue;
                        v = v+temp
                else:
                    group = 1
                    if func_break[i].isdigit()==1:
                        v += func_break[i]+","
                    else:
                        if func_break[i].find("\"") != -1:
                            v += func_break[i]+","
                        elif func_break[i].find("\'") != -1:
                            v += func_break[i]+","
                        else:
                            v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                            cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                i = i + group
            col1 = v.split(",")
            col1.remove('')
              
            for i in range(len(col1)):
                if i < 3:
                    if col1[i] != "''":
                        var = var + col1[i]+","
                    else:
                        continue
                else:
                    if col1[i].find("(") != -1:
                        string = 1;
                    else:
                        col1[i]=col1[i].split(".")[1]
            gp_cols = cols[:-1]
            tot = items.upper()+"("+var[:-1]+")"
            if len(col1) > 3:
                if string != 1:
                    tot = tot +" "+' '.join(col1[3:])
                    group = len(func_break)-1
                else:
                    tot = tot+","+' '.join(col1[3:])
            logging.debug(' -- '+ items.upper()+' -- '+tot)      
            return tot , gp_cols, group 
            
            
                            
        elif items.upper() == 'TRIM' or items.upper() == 'LTRIM' or items.upper() == 'RTRIM':
                group = 2
                t1 =''
                v = ''
                col1 = []
                
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                if val.find("group by") == -1:
                        sl = len(func_break)
                else:
                        sl = func_break.index("group")
                i = 1
                while i < len(func_break)-1:
                    if func_break[1] in string_functions:
                        group = 2
                        t = ' '.join(func_break[i:])
                        t1 , gp_cols , group = string_opr(input_list, t , t)
                        cols = cols+gp_cols+","
                        i = i+ group
                        logging.debug(' -- '+ items.upper()+' -- '+t1)
                        return items.upper()+"(" + t1 +")", gp_cols , group                        
                    else:
                        group = 1
                        v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                        cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                        i = i + 1;
                
                tot = items.upper()+"("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1] + ")"
                if len(func_break) > 2:
                    tot = tot +" "+' '.join(func_break[2:])
                    group = len(func_break)-1
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols, group 
                    
        elif items.upper() == 'ISNULL':
                col1 = []
                if val.find("where") == -1:
                    if val.find("group by") == -1:
                        sl = len(func_break)
                    else:
                        sl = func_break.index("group")
                else:
                    sl = func_break.index("where")

                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]

                flag4 = 0
                for items in func:
                    if items in maths_list:
                        ind = func.index(items)
                        sub_func = func[ind:]
                        sub_fun = ' '.join(sub_func)
                        derived_port , gp_cols,group = maths_opr(input_list,sub_fun)
                        flag4 = 1
                        tot = "isnull("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func[1] + ")" + derived_port
                
                if flag4 == 0 :       
                    tot = "isnull("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func[1] + ")"
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group
                    
                    
        elif items.upper() == 'ISNOTNULL':
                if val.find("where") == -1:
                    if val.find("group by") == -1:
                        sl = len(func_break)
                    else:
                        sl = func_break.index("group")
                else:
                    sl = func_break.index("where")
                    
                flag4 = 0
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                
                for items in func:
                    if items in maths_list:
                        ind = func.index(items)
                        sub_func = func[ind:]
                        sub_fun = ' '.join(sub_func)
                        derived_port , gp_cols,group = maths_opr(input_list,sub_fun)
                        flag4 = 1
                        tot = "isnotnull("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func[1] + ")" + derived_port
                
                if flag4 == 0 :       
                    tot = "isnotnull("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func[1] + ")"
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group    

                    
        elif items.upper() == 'LENGTH':
                v = ''
                if val.find("where") == -1:
                    if val.find("group by") == -1:
                        sl = len(func_break)
                    else:
                        sl = func_break.index("group")
                else:
                    sl = func_break.index("where")
                flag4 = 0
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                
                i = 1
                while i < len(func_break)-1:
                    if func_break[1] in string_functions:
                        group = 2
                        t = ' '.join(func_break[i:])
                        t1 , gp_cols , group = string_opr(input_list, t , t)
                        cols = cols+gp_cols+","
                        i = i+ group
                        logging.debug(' -- '+ items.upper()+' -- '+t1)
                        return "LENGTH(" + t1 +")", gp_cols, group
                    elif func_break[i] in maths_list:
                        sub_func = func[i:]
                        sub_fun = ' '.join(sub_func)
                        derived_port , gp_cols,group = maths_opr(input_list,sub_fun)
                        flag4 = 1
                        tot = "LENGTH("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func[1] + ")" + derived_port    
                        cols = cols+gp_cols+","
                        i = i+1
                        gp_cols = cols[:-1]
                        logging.debug(' -- '+ items.upper()+' -- '+tot)
                        return tot , gp_cols ,group
                    else:
                        group = 1
                        v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                        cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                        i = i + 1;
                        
                if flag4 == 0 : 
                    tot = "LENGTH("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1] + ")"
                
                if len(func_break) > 2:
                    tot = tot +" "+' '.join(func_break[2:])
                    group = len(func_break)-1
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols, group 
                
        elif items.upper() == 'UPPER' or items.upper() == 'LOWER':
                group = 2
                t1 =''
                v = ''
                col1 = []
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                if val.find("group by") == -1:
                        sl = len(func_break)
                else:
                        sl = func_break.index("group")
                i = 1
                while i < len(func_break)-1:
                    if func_break[1] in string_functions:
                        group = 2
                        t = ' '.join(func_break[i:])
                        t1 , gp_cols , group = string_opr(input_list, t , t)
                        cols = cols+gp_cols+","
                        i = i+ group
                        logging.debug(' -- '+ items.upper()+' -- '+t1)
                        return items.upper()+"(" + t1 +")", gp_cols, group
                        
                    else:
                        group = 1
                        v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                        cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                        i = i + 1;            
                tot = items.upper()+"("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1] + ")"
                if len(func_break) > 2:
                    tot = tot +" "+' '.join(func_break[2:])
                    group = len(func_break)-1
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols, group
        
                         
        elif items.upper() == 'COALESCE':
            col1 = []
            if val.find("group by") == -1:
                sl = len(func_break)
            else:
                sl = func_break.index("group")
            v = "COALESCE("
            #print(v)
            for i in range(1,sl) :
                if(i >= 1): 
                    v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                tot = v[:-1]+")"
                cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
            gp_cols = cols[:-1]
            logging.debug(' -- '+ items.upper()+' -- '+tot)
            return tot , gp_cols,group
 
        elif items.upper() == 'REPLACE':
            if val.find("group by") == -1:
                    sl = len(func_break)
            else:
                    sl = func_break.index("group")
            group = 4
            t1 = ''
            tot = ''
            col1 = []
            var = ''
            v = '' 
            i = 1;
            while i <= len(func_break)-1:
                var = ''

                if func_break[i] in string_functions:
                    t = ' '.join(func_break[i:])
                    t1 , gp_cols , group = string_opr(input_list, t , t)
                    cols = cols+gp_cols+","
                    func_break = t1.split(" ")
                    if len(func_break) == 1:
                        v = v + func_break[0]+","
                        break
                        
                    elif len(func_break) > 1:
                        for n in range(0 , len(func_break)):
                            if var == 'up':
                                i = -group
                                continue;
                            if n == 0:
                                temp = func_break[0]+","
                                i = i+ group
                                continue
                            if func_break[n] not in string_functions:
                                if func_break[n].isdigit()==1:
                                    temp = temp +func_break[n] +","
                                else:
                                    if func_break[n].find("\"") != -1:
                                        temp = temp + func_break[n] +","
                                    elif func_break[n].find("\'") != -1:
                                        temp = temp + func_break[n] +","
                                    else:
                                        cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n]+","
                                        temp = temp + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n] +","
                            else:
                                var = ' '.join(func_break[n:])
                                func_break = var.split()
                                var = 'up'
                                v = v + temp
                                temp = ''
                                #break;
                                continue;

                        v = v+temp
                else:
                    group = 1
                    if func_break[i].isdigit()==1:
                        v += func_break[i]+","
                    else:
                        if func_break[i].find("\"") != -1:
                            v += func_break[i]+","
                        elif func_break[i].find("\'") != -1:
                            v += func_break[i]+","
                        else:
                            v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                            cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                i = i + group
            col1 = v.split(",")
            col1.remove('')
                
            for i in range(len(col1)):
                if i < 3:
                    var = var + col1[i]+","
                else:
                    if col1[i].isdigit == 0:
                        col1[i]=col1[i].split(".")[1]
            gp_cols = cols[:-1]
            tot = "REPLACE("+var[:-1]+")"
            if len(col1) > 3:
                    tot = tot +" "+' '.join(col1[3:])
                    group = len(func_break)-1
            logging.debug(' -- '+ items.upper()+' -- '+tot)
            return tot , gp_cols, group 
                                    
        elif items.upper() == 'SPLIT':
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                if func_break[2] == "''":
                    tot = "SPLIT("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1] +","+"\" \")"
                else:
                    tot = "SPLIT("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1] +","+func_break[2]+")"    
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group
                                    
        elif items.upper() == 'CAST':
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                tot = "CAST("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1] +" as "+func_break[2]+")"    
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group
        
        elif items.upper() == 'MD':
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                tot = "MD5("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1]+")"   
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group
        
        
        elif items.upper() == 'ARRAY':
                gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
                tot = "ARRAY_CONTAINS("+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[1]+","+func_break[2]+")"    
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group
            
        elif items.upper() == 'LPAD' or items.upper() == 'RPAD':
            if val.find("group by") == -1:
                    sl = len(func_break)
            else:
                    sl = func_break.index("group")
            group = 4
            t1 = ''
            tot = ''
            col1 = []
            var = ''
            v = '' 
            i = 1;
            while i <= len(func_break)-1:
                var = ''

                if func_break[i] in string_functions:
                    t = ' '.join(func_break[i:])
                    t1 , gp_cols , group = string_opr(input_list, t , t)
                    cols = cols+gp_cols+","
                    func_break = t1.split(" ")
                    if len(func_break) == 1:
                        v = v + func_break[0]+","
                        break
                        
                    elif len(func_break) > 1:
                        for n in range(0 , len(func_break)):
                            if var == 'up':
                                i = -group
                                continue;
                            if n == 0:
                                temp = func_break[0]+","
                                i = i+ group
                                continue
                            if func_break[n] not in string_functions:
                                if func_break[n].isdigit()==1:
                                    temp = temp +func_break[n] +","
                                else:
                                    if func_break[n].find("\"") != -1:
                                        temp = temp + func_break[n] +","
                                    elif func_break[n].find("\'") != -1:
                                        temp = temp + func_break[n] +","
                                    else:
                                        cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n]+","
                                        temp = temp + input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[n] +","
                            else:
                                var = ' '.join(func_break[n:])
                                func_break = var.split()
                                var = 'up'
                                v = v + temp
                                temp = ''
                                #break;
                                continue;

                        v = v+temp
                else:
                    group = 1
                    if func_break[i].isdigit()==1:
                        v += func_break[i]+","
                    else:
                        if func_break[i].find("\"") != -1:
                            v += func_break[i]+","
                        elif func_break[i].find("\"") != -1:
                            temp = temp + func_break[i] +","
                        else:
                            v += input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                            cols = cols +input_list['SOURCE_TABLE'].split(" ")[1]+"."+func_break[i]+","
                i = i + group
            col1 = v.split(",")
            col1.remove('')
                
            for i in range(len(col1)):
                if i < 3:
                    var = var + col1[i]+","
                else:
                    col1[i]=col1[i].split(".")[1]
            gp_cols = cols[:-1]
            tot = items.upper()+"("+var[:-1]+")"
            if len(col1) > 3:
                    tot = tot +" "+' '.join(col1[3:])
                    group = len(func_break)-1
            logging.debug(' -- '+ items.upper()+' -- '+tot)
            return tot , gp_cols, group 
        
        elif items.upper() == 'CEIL':
                global flag3
                gp_cols = input_list["SOURCE_TABLE"].split(" ")[1]+"."+ func_break[1]
                flag3 = 0
                for items in func:
                    if items in maths_list:
                        ind = func.index(items)
                        sub_func = func[ind:]
                        sub_fun = ' '.join(sub_func)
                        derived_port , gp_cols,group = maths_opr(input_list,sub_fun)
                        #print(derived_port+"///")
                        flag3 = 1
                        if func[1].isdigit() == 0:
                            tot = "CEIL("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func[1]+")" + derived_port
                        else:
                            tot = "CEIL("+ func[1]+")" + derived_port
                        break
                if flag3 == 0:
                    if func[1].isdigit() == 0:
                        tot = "CEIL("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func[1]+")"
                    else:
                        tot = "CEIL("+ func[1]+")"
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group


        elif items.upper() == 'ROUND':
                global flag2
                gp_cols = input_list["SOURCE_TABLE"].split(" ")[1]+"."+ func[1]
                cols = cols + input_list["SOURCE_TABLE"].split(" ")[1]+"." + func[1]+","
                flag2 = 0
                if (len(func) > 2):
                    for items in func:
                        #print(items)
                        if items in maths_list:
                            ind = func.index(items)
                            #print(ind)
                            sub_func = func[ind:]
                            sub_fun = ' '.join(sub_func)
                            #print(sub_fun)
                            derived_port , gp_cols,group = maths_opr(input_list,sub_fun)
                            cols = cols+gp_cols+","
                            flag2 = 1
                            if (ind - 1) > 1 :
                                if func[1].isdigit() == 0:
                                    tot = "ROUND("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func[1]+","+func[2]+")" + derived_port
                                else:
                                    tot = "ROUND("+func[1]+","+func[2]+")" + derived_port   
                            else:
                                if func[1].isdigit() == 0:
                                    tot = "ROUND("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func[1]+")" + derived_port
                                else:
                                    tot = "ROUND("+ func[1]+")" + derived_port
                            break
                    if flag2 == 0:
                        if func[1].isdigit() == 0:
                            tot = "ROUND("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func[1]+","+func[2]+")"
                            #print(tot)
                        else:
                            tot = "ROUND("+ func[1]+","+func[2]+")"
                else:
                    if func[1].isdigit() == 0:
                        tot = "ROUND("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func[1]+")"
                    else:
                        tot = "ROUND("+ func[1]+")"
                gp_cols=cols[:-1]
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group
        
        elif items.upper() == 'IFNULL':
                tot , gp_cols,group = nvl_fnc(input_list)
                logging.debug(' -- '+ items.upper()+' -- '+tot)
                return tot , gp_cols,group
                
def aggregate_fun(input_list,val):
    group = 1
    func_break = val.split()
    gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[1]
    for items in func_break:
        if items.upper() == 'COUNT' or items.upper() == 'SUM' or items.upper() == 'AVG' or items.upper() == 'MAX' or items.upper() == 'MIN' :
            try:
                s1 = func_break.index("group")
            except:
                s1 = len(func_break)
            flag2 = 0
            if (len(func_break) > 2):
                for items in func_break:
                    if items in maths_list:
                        ind = func_break.index(items)
                        sub_func = func_break[ind:]
                        sub_fun = ' '.join(sub_func)
                        derived_port , gp_cols,group = maths_opr(input_list,sub_fun)
                        flag2 = 1
                        try:
                            if func_break.index('DISTINCT') > 0:
                                if (ind - 1) > 2 :
                                    tot = func_break[0].upper()+"(" + func_break[1]+"("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[2]+","+func_break[3] + derived_port+"))"
                                else:
                                    tot = func_break[0].upper()+"("  + func_break[1]+"("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[2] + derived_port+"))"
                                gp_cols = input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[2]+","+gp_cols
                        except:
                            if (ind - 1) > 1 :
                                tot = func_break[0].upper()+"("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[1]+","+func_break[2] + derived_port+")"
                            else:
                                tot = func_break[0].upper()+"("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[1] + derived_port+")"
                            gp_cols =input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[1]+","+gp_cols
                if flag2 == 0:
                    try:
                        if func_break.index('DISTINCT') > 0:
                            tot = func_break[0].upper()+"(DISTINCT "+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[2]+")"
                    except:
                        tot = func_break[0].upper()+"("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[1]+","+func_break[2]+")"
            else:
                
                tot = func_break[0].upper()+"("+input_list["SOURCE_TABLE"].split(" ")[1]+"." + func_break[1]+")"
            logging.debug(' -- '+ items.upper()+' -- '+tot)
            return tot , gp_cols,group           


def nvl_fnc(input_list):
    #print(input_list,"inputtttt")
    group = 0
    case_stmt = input_list['TRANSFORMATION'].split(" ")
    i1 = case_stmt.index("ifNULL")
    i2 = case_stmt.index("default")
    i3 = i2-i1
    sub_list = []
    i4 = i1+1
    i = 1
    
    gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']
    for i in range(i3):
        sub_list.append(case_stmt[i4+i])
    for items in sub_list:
        if items.upper() in key_Words:
            
            return_qry = choose_function(items, input_list)
            nq_list = return_qry.split(" ")   #Select Loc_data.Purchase_Order as ADDRESS_ID from left join Loc_data on ADDRESS_ID.Cust_id=Loc_data.Cust_id
            a1 = nq_list.index("as")
            a2 = nq_list.index("from")
            logging.debug(' -- '+items.upper()+ ' -- '+ "NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+nq_list[a1-1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i2+1]+") AS "+input_list['TARGET_COLUMN'])
            #logging.info(' -- '+items.upper()+' -- '+'EXIT the function')
            return "NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+nq_list[a1-1]+","+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i2+1]+") AS "+input_list['TARGET_COLUMN'] , gp_cols,group
        else:
            if val.find("group by") == -1:
                logging.debug(' -- '+items.upper()+ ' -- '+ "NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i1+1]+","+case_stmt[i2+1]+")")
                #logging.info(' -- '+items.upper()+' -- '+'EXIT the function')
                return "NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i1+1]+","+case_stmt[i2+1]+")",gp_cols,group
            elif val.find("group by") != -1:
               b = val.index("group by")
               try:
                   h = val.index("having")
                   c1 = val.index("(")
                   #print("Select NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i1+1]+","+case_stmt[i2+1]+") AS "+input_list['TARGET_COLUMN']+" from "+ input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[h+7:])
                   logging.debug(' -- '+items.upper()+ ' -- '+"NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i1+1]+","+case_stmt[i2+1]+")")
                   #logging.info(' -- '+items.upper()+' -- '+'EXIT the function')
                   return "NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i1+1]+","+case_stmt[i2+1]+")",gp_cols,group
               except:
                   logging.debug(' -- '+items.upper()+ ' -- '+"NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i1+1]+","+case_stmt[i2+1]+")")
                   #logging.info(' -- '+items.upper()+' -- '+'EXIT the function')
                   return "NVL("+input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[i1+1]+","+case_stmt[i2+1]+")",gp_cols,group


def case_when(input_list,transformation):
    tmp=''
    total =''
    group = 1
    case_stmt = transformation.split(" ")
    tmp=''
    tmp1=''
    list1=[]
    i_when = case_stmt.index('when')
    i_else = case_stmt.index('else')
    i_end = case_stmt.index('end')
    i_then = case_stmt.index('then')
    const_find = 0
    #i = 3;
    #for n in range(len(case_stmt)):
    n = 1
    while n < len(case_stmt):
        #print(n,case_stmt[n])
        if case_stmt[n] == 'and' or case_stmt[n] =='or':
            
            case_stmt[n] = case_stmt[n].upper()+" " +input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']
            list1.append(case_stmt[n])
        #elif n == 2:
        #        list1.append(input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" "+case_stmt[n])
        else:
            if case_stmt[n] in string_functions:
                if n < i_then:
                    tmp = ' '.join(case_stmt[n:i_then])
                    n = i_then-1
                elif n < i_else:
                    tmp = ' '.join(case_stmt[n:i_else])
                    n = i_else-1
                    #print("else",case_stmt[n])
                else:
                    tmp = ' '.join(case_stmt[n:i_end])
                    n = i_end-1
                tmp1,gp_cols,group = string_opr(input_list,tmp,tmp)
                list1.append(tmp1)
                #print(n, i_else)
            elif case_stmt[n] in aggregate_list:
                if n < i_then:
                    tmp = ' '.join(case_stmt[n:i_then])
                    n = i_then-1
                elif n < i_else:
                    tmp = ' '.join(case_stmt[n:i_else])
                    n = i_else-1
                else:
                    tmp = ' '.join(case_stmt[n:i_end])
                    n = i_end-1
                tmp1,gp_cols,group = aggregate_fun(input_list,tmp)
                #list1.append(tmp1.split()[0])
                list1.append(tmp1)
            else:
                if n == i_when:
                    list1.append('when')
                    list1.append(input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" " +case_stmt[n+1])
                    n = i_when+1
                elif n < i_then:
                    if case_stmt[n].find('"') != -1 or case_stmt[n].find('\'') != -1 :
                        list1.append(''.join(case_stmt[n]))
                        const_find = const_find+1
                    elif case_stmt[n].upper() == 'LIKE':
                        list1.append(''.join(case_stmt[n]))
                    elif case_stmt[n].isdigit() == 1:
                        list1.append(''.join(case_stmt[n]))
                    elif const_find%2 == 1: 
                        list1.append(''.join(case_stmt[n]))
                    else:
                        list1.append(input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[n])
                elif n == i_then:        
                    list1.append("then")
                    
                elif n > i_then and n <i_else:
                    if ' '.join(case_stmt[n:i_else]).find('when') != -1:
                        i_when = case_stmt[n:i_else].index('when') + n
                        if case_stmt[n].find('"') != -1 or case_stmt[n].find('\'') != -1:
                            list1.append(' '.join(case_stmt[n:i_when]))
                        elif case_stmt[n].find('.') != -1:
                            list1.append(' '.join(case_stmt[n:i_when]))
                        else:
                            list1.append(input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[n])
                        
                        i_then = case_stmt[n:i_else].index('then') + n
                        #list1.append(input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN'])
                        n = i_when-1
                    else:
                        if case_stmt[n].find('"') != -1 or case_stmt[n].find('\'') != -1:
                            list1.append(' '.join(case_stmt[n:i_else]))
                        elif case_stmt[n].find('.') != -1:
                            list1.append(' '.join(case_stmt[n:i_else]))
                        else:
                            list1.append(input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[n])
                        n = i_else-1
                elif n == i_else:
                    list1.append("else")
                elif n > i_else and n < i_end:
                    if case_stmt[n].find('"') != -1 or case_stmt[n].find('\'') != -1:
                        list1.append(' '.join(case_stmt[n:i_end]))
                    elif case_stmt[n].find('.') != -1:
                        list1.append(' '.join(case_stmt[n:i_end]))
                    else:
                        list1.append(input_list['SOURCE_TABLE'].split(" ")[1]+"."+case_stmt[n])
                    n = i_end-1
                elif n == i_end:
                    list1.append("end")
        n=n+1
    for i in range(len(list1)):
        total = total + list1[i]+" "
    
    logging.debug(' -- '+"Select CASE when "+total+"AS "+input_list['TARGET_COLUMN']+" from "+ input_list['SOURCE_TABLE'])
    return "Select CASE "+total+"AS "+input_list['TARGET_COLUMN']+" from "+ input_list['SOURCE_TABLE'] , input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN'] , 0   
                #if (val.find("where") == -1 and val.find("group by") == -1):
                #logging.debug(' -- '+items.upper()+ ' -- '+"Select "+case_stmt[0]+" "+case_stmt[1]+" "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+val[10:]+" AS "+input_list['TARGET_COLUMN']+" from "+ input_list['SOURCE_TABLE'])

def lookup(input_list,row,val):  #['Lookup', 'Loc_data', 'ON', 'Cust_id', '=', 'India', 'then']
    #logging.info('ENTER the function ')    
    group = 0
    list1 = []
    list2 = []
    list3 = []
    list4 =[]
    wh_find = 0
    var=''
    length = 0
    
    if type(input_list['BUSINESS_RULE']) is float:
        length = 1
    else:
        rule_find = input_list['BUSINESS_RULE'].split()
        var1 = rule_find[:]
    if length != 1:
        for j in range(len(var1)):
            if (var1[j] != 'where'):
                if(var1[j]=='=' or var1[j] == '>' or var1[j] == '<' or var1[j] == '<>' or var1[j] == '<=' or var1[j] == '>=' ):
                    abc=base_table.split(" ")[1]+"."+var1[(j-1)]+var1[j]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+var1[(j+1)]
                    list1.append(abc)
                    
                elif(var1[j]=="AND" or var1[j]=="OR"):
                    list2.append(var1[j])
            else:
                for i in range((j+1), len(var1)):
                    if var1[i] == 'AND':
                        list3.append(var1[i])
                    elif var1[i] == '=' or var1[i] == '>' or var1[i] == '<' or var1[i] == '<>':
                        t = base_table.split(" ")[1]+"."+var1[(i-1)] + var1[i] + var1[(i+1)]+" "
                        list4.append(t)
                xyz = ''
                for num in range(len(list3)):
                    xyz = xyz +" "+ list4[num] + list3[num]
                    logging.debug(' -- LOOKUP conditions -- '+ xyz)
                    
                wh_find = 1  
                xyz = xyz +" "+ list4[-1]
                break
        for k in range(len(list2)):
            var = var+" "+list1[k]+" "+list2[k]
        logging.debug(' -- LOOKUP conditions -- '+ abc)
    out_sql1 = ''   
    if type(input_list['TRANSFORMATION']) is not float and input_list['TRANSFORMATION'].find("group") == -1:
        fr = 0
        final = ''
        #print(val)
        out_sql1, gp_cols,group = derived(input_list,row,val)
        fr = out_sql1.find('from')
        if length == 1:
            final = out_sql1[:fr+5] + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON"
        else:
            #print(length)
            final = out_sql1[:fr+5] + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON"+var +" "+list1[-1] 
        if type(val) is float:
            final = final
        elif val.find("group by") != -1:
                 b = val.index("group by")
                 try:
                     h = val.index("having")
                     having_break = (''.join(val[h+7:]))
                     abc = having_break.split(" ")
                     if abc[0] in aggregate_list:
                         derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                         having_cond = ' '.join(abc[2:])
                         final = final +" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond
                     else:
                        c1 = val.index("(")
                        #print("Select "+lookup_field[3]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + join +" "+lookup_field[1].upper()+" "+  lookup_field[2]+" "+lookup_field[3] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[h+7:])
                        final = final +" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:]
                 except:   
                     #print("Select "+lookup_field[3]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + join +" "+lookup_field[1].upper()+" "+  lookup_field[2]+" "+lookup_field[3] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                     final = final+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:]
        logging.debug(' -- '+final)
        #logging.info('EXIT the function')
        return final, gp_cols,group
        
    if wh_find == 0:
        try:
            gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']
        except:
            gp_cols ='f1'
            
        if (gp_cols != 'f1'):
            if type(val) is float:
                #print("length = 0",length)
                if length == 1:
                    logging.debug(' -- '+"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON")
                    #print("Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + join +" "+lookup_field[1].upper()+" " +input_list['SOURCE_TABLE']+ " ON"+var +" "+list1[-1])
                    #logging.info('EXIT the function')
                    return \
                       "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON" , gp_cols,group
                else:
                    logging.debug(' -- Select ' + input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON"+var +" "+list1[-1])
                    #logging.info('EXIT the function')
                    return \
                       "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON"+var +" "+list1[-1] , gp_cols,group
            elif val.find("group by") != -1:
                 b = val.index("group by")
                 try:
                     h = val.index("having")
                     having_break = (''.join(val[h+7:]))
                     abc = having_break.split(" ")
                     if abc[0] in aggregate_list:
                         derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                         having_cond = ' '.join(abc[2:])
                         if length == 1:
                             logging.debug(' -- '+"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE']+ " ON"+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond)
                             #logging.info('EXIT the function')
                             return \
                               "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE']+ " ON" +" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond, gp_cols ,group
                         else:
                             logging.debug(' -- '+"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond)
                             #logging.info('EXIT the function')
                             return \
                               "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond, gp_cols ,group
                     else:
                         c1 = val.index("(")
                         if length == 1:
                             logging.debug(' -- '+"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE']+ " ON"+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                             #logging.info('EXIT the function')
                             return \
                               "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE']+ " ON" +" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:] , gp_cols ,group
                             
                         else:
                             logging.debug(' -- '+"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                             #logging.info('EXIT the function')
                             return \
                               "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:] , gp_cols ,group
                 except:   
                     if length == 1:
                         logging.debug(' -- '+"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " +input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] +" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                         #logging.info('EXIT the function')
                         return \
                           "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " +input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:] , gp_cols,group
                     else:
                         logging.debug(' -- '+"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " +input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                         #logging.info('EXIT the function')
                         return \
                           "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " +input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:] , gp_cols,group
        else:
            if type(val) is float:
                logging.debug(' -- '+" from " + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON"+var +" "+list1[-1])
                #logging.info('EXIT the function')
                return \
                   "from " + input_list['JOINS'] +" JOIN " +input_list['SOURCE_TABLE']+ " ON"+var +" "+list1[-1] , gp_cols,group
            elif val.find("group by") != -1:
                 b = val.index("group by")
                 try:
                     h = val.index("having")
                     having_break = (''.join(val[h+7:]))
                     abc = having_break.split(" ")
                     if abc[0] in aggregate_list:
                         derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                         having_cond = ' '.join(abc[2:])
                         logging.debug(' -- '+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond)
                         #print("Select "+lookup_field[3]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + join +" "+lookup_field[1].upper()+" "+  lookup_field[2]+" "+lookup_field[3] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[h+7:])
                         #logging.info('EXIT the function')
                         return "from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond , gp_cols ,group
                     else:
                         c1 = val.index("(")
                         logging.debug(' -- '+" from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                         #print("Select "+lookup_field[3]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + join +" "+lookup_field[1].upper()+" "+  lookup_field[2]+" "+lookup_field[3] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[h+7:])
                         #logging.info('EXIT the function')
                         return "from " + input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:] , gp_cols ,group
                 except:   
                     logging.debug(' -- '+"from " +input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                     #print("Select "+lookup_field[3]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from " + join +" "+lookup_field[1].upper()+" "+  lookup_field[2]+" "+lookup_field[3] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
                     #logging.info('EXIT the function')
                     return \
                       "from " +input_list['JOINS'] +" JOIN " + input_list['SOURCE_TABLE'] + " ON"+var +" "+list1[-1]+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:] , gp_cols,group               

def row_number(input_list,val):
    #logging.info('ENTER the function ')
    #print(val)
    group = 0
    func_break = input_list['TRANSFORMATION'].split()
    #print(func_break)
    #len(func_break)
    function=''
    function = (val.split(" ")[0])
    if (val.find("partition") != -1):
        gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[3]
        if (val.find("order") != -1):            
            tot = function +"() over(PARTITION BY "+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[3]+" ORDER BY "\
             +input_list['SOURCE_TABLE'].split(" ")[1]+"." +func_break[6]+") AS " 
        else:
            tot = function +"() over(PARTITION BY "+input_list['SOURCE_TABLE'].split(" ")[1]+"." + func_break[3]+") AS "
    elif (val.find("order") != -1):
        gp_cols = input_list['SOURCE_TABLE'].split(" ")[1]+"."+ func_break[3]       
        tot = function +"() over(ORDER BY "+input_list['SOURCE_TABLE'].split(" ")[1]+"." +func_break[3]+") AS "
    else:
        gp_cols = 'f1'
        tot = function +"() over() AS "
    if (val.find("group by") == -1):
        tot1 ="Select "+tot+input_list['TARGET_COLUMN']+" from "+ base_table
        logging.debug(' -- '+tot1)
        #logging.info('EXIT the function')
        return tot1 , gp_cols , group
    elif((val.find("group by") != -1)):
        b = val.index("group by")
        try:
          h = val.index("having")
          having_break = (''.join(val[h+7:]))
          abc = having_break.split(" ")
          if abc[0] in aggregate_list:
              derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
              having_cond = ' '.join(abc[2:])
              tot1 = "Select "+tot+input_list['TARGET_COLUMN']+" from "+ input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+derived_portt+having_cond
          else:
              c1 = val.index("(")          
              tot1 = "Select "+tot+input_list['TARGET_COLUMN']+" from "+ input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:]
          logging.debug(' -- '+tot1)
          #logging.info('EXIT the function')
          return tot1 , gp_cols,group
        except:
          tot1 = "Select "+tot+input_list['TARGET_COLUMN']+" from "+ input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:]
          logging.debug(' -- '+tot1)
          #logging.info('EXIT the function')
          return tot1 , gp_cols , group


def condition(input_list):
    gp_cols = 'f1'
    group = 2
    tot1 =''
    if type(input_list['SOURCE_COLUMN']) is float:
        tot1 = "Select '' AS " + input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']
    else:
        if input_list['SOURCE_COLUMN'].find("*") != -1:
            tot1 = "Select " +input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" from "+input_list['SOURCE_TABLE']
        elif input_list['SOURCE_COLUMN'].find("\"") != -1 or input_list['SOURCE_COLUMN'].find("\'") != -1:
            if type(input_list['SOURCE_TABLE']) is float:
                tot1 = "Select " +input_list['SOURCE_COLUMN']+" AS " + input_list['TARGET_COLUMN']
            else:
                tot1 = "Select " +input_list['SOURCE_COLUMN']+" AS " + input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']
    logging.debug(' -- '+tot1)
    return tot1 , gp_cols , group


def one2one(input_list,val):
    group = 0
    if type(val) is float:
        logging.debug(' -- ' +"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE'])
        #logging.info('EXIT the function')
        return "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE'] , input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN'] ,group
    
    elif val.find("group by") != -1:
        b = val.index("group by")
        try:
            h = val.index("having")
            having_break = (''.join(val[h+7:]))
            abc = having_break.split(" ")
            if abc[0].upper() in aggregate_list:
                derived_portt,gp_cols,group = aggregate_fun(input_list,' '.join(abc[0:2]))
                #logging.info(' -- '+'EXIT the function')
                having_cond = ' '.join(abc[2:])
                return "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN'] + " AS " + input_list['TARGET_COLUMN'] + " from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+ derived_portt +having_cond , gp_cols  ,group
            else:
                c1 = val.index("(")
                logging.debug(' -- ' +"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:])
                return "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:h]+" having "+val[h+7:c1+1]+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[c1+1:],input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN'],group
        except:
            logging.debug(' -- ' +"Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:])
            return "Select "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN']+" AS "+input_list['TARGET_COLUMN']+" from "+input_list['SOURCE_TABLE']+" group by "+input_list['SOURCE_TABLE'].split(" ")[1]+"."+val[b+9:],input_list['SOURCE_TABLE'].split(" ")[1]+"."+input_list['SOURCE_COLUMN'],group
        
def choose_function(function_name, input_string,row,val):
        logging.debug('Function names: ')
		logging.debug(function_name)
        buss_rule = input_string
        if function_name.upper() == 'ONE-2-ONE':
                out_sql , gp_cols,group = one2one(buss_rule,val)
                return out_sql , gp_cols,group                
        elif function_name.upper() == 'JOINS':
                out_sql, gp_cols,group = lookup(buss_rule,row,val)
                return out_sql, gp_cols,group
        elif function_name.upper() == 'DERIVED':
                out_sql, gp_cols,group = derived(buss_rule,row,val)
                return out_sql, gp_cols,group
        elif function_name.upper() == 'CONDITION':
                out_sql, gp_cols,group = condition(buss_rule)
                return out_sql, gp_cols,group

def etl_rule(target_column, input_string,row ,file_out,val):
    Func_List = []
    if type(input_string['BUSINESS_RULE']) is float and type(input_string['JOINS']) is float:
        Func_List.append("CONDITION")
    else:
        if type(input_string['JOINS']) is not float:
            Func_List.append("JOINS")
        else:
            Input_list = input_string['BUSINESS_RULE'].split(" ")
            for items in Input_list:
                if items.upper() in key_Words:
                    Func_List.append(items.upper())
    if Func_List != []:
            sql_query, gp_cols,group = choose_function(Func_List[0], input_string,row,val)
            output_sql.append(sql_query)
            #print(sql_query)
            file_out.write(sql_query+ "\n")
            temp_col.append(gp_cols)
            grp.append(group)
    
def file_write(f_out):
    templist = []
    templist1 = []
    templist2 = []
    templist3 = []
    templist4 = []
    templist5 = []
    templist6 = []
    temp = []
    from_find = 0

    for element in formatted_sql:
        if element.isspace() or element == '' or element == " " or element == '  ':
            formatted_sql.remove(element)
        elif element == 'from':
            from_find = 1

    if from_find != 1:
        temp_sql = formatted_sql[:-2]
        sp1 = formatted_sql.index(';')
        v2 = formatted_sql[sp1-1][:-1]
        temp_sql.append(v2)
        for_sql = temp_sql
        
    else:    
        fr1 = formatted_sql.index('from')
        temp_sql = formatted_sql[:fr1-1]
        v1 = formatted_sql[fr1-1][:-1]
        temp_sql.append(v1)
        for items in formatted_sql:
            if items.find('where ') != -1:
                val = 1
                break
            elif items.find('group by') != -1:
                val = 2
                break
            else:
                val = 0
                
        if(val == 1):
                wh = formatted_sql.index('where ')
                temp_sql_1 = formatted_sql[fr1:wh+1]
                if "group by" in formatted_sql:
                    if "having" not in formatted_sql:
                        p = formatted_sql.index("group by")
                        templist = formatted_sql[wh+1:p]
                        for item in templist:
                            if "AND" in item:
                                if item not in temp:
                                    temp.append(item)
                        for item in templist:
                            if item not in templist2:
                                if "AND" not in item:
                                    templist2.append(item)        
                        for item1 in temp:
                            for items in temp:
                                a = items.index(" AND")
                                b = items.index(" OR")
                                c = item1.index(" AND")
                                d = item1.index(" OR")
                                if ((item1[c+5:d-1] == items[1:a] or item1[c+5:d-1] == items[a+5:b-1]) and (item1[1:c] == items[1:a] or item1[1:c] == items[a+5:b-1])):
                                    if item1 != items:
                                        if item1 not in templist1:
                                            templist1.append(item1)
                        for items in temp:
                            if items not in templist1:
                                templist2.append(items)
                        for item1 in templist1:
                            for items in templist1[templist1.index(item1)+1:]:
                                a = items.index(" AND")
                                b = items.index(" OR")
                                c = item1.index(" AND")
                                d = item1.index(" OR")
                                if ((item1[c+5:d-1] == items[1:a] or item1[c+5:d-1] == items[a+5:b-1]) and (item1[1:c] == items[1:a] or item1[1:c] == items[a+5:b-1])):
                                    if item1 not in templist2:
                                        if item1 != items:
                                            templist2.append(item1)
                        temp_sql_11 = templist2[ :-1]
                        v1 = templist2[-1][:-3]
                        temp_sql_11.append(v1)
                        templist3 = formatted_sql[p:]
                        for item in templist3:
                            if item not in templist4:
                                templist4.append(item)
                        temp_sql_111 = templist4[ :-2]
                        sp1 = templist4.index(';')
                        v2 = templist4[sp1-1][:-1]
                        temp_sql_111.append(v2)
                        for_sql = temp_sql +temp_sql_1+temp_sql_11+temp_sql_111
    
                    else:
                        p = formatted_sql.index("group by")
                        templist = formatted_sql[wh+1:p]
                        for item in templist:
                            if "AND" in item:
                                if item not in temp:
                                    temp.append(item)
                        for item in templist:
                            if item not in templist2:
                                if "AND" not in item:
                                    templist2.append(item)        
                        for item1 in temp:
                            for items in temp:
                                a = items.index(" AND")
                                b = items.index(" OR")
                                c = item1.index(" AND")
                                d = item1.index(" OR")
                                if ((item1[c+5:d-1] == items[1:a] or item1[c+5:d-1] == items[a+5:b-1]) and (item1[1:c] == items[1:a] or item1[1:c] == items[a+5:b-1])):
                                    if item1 != items:
                                        if item1 not in templist1:
                                            templist1.append(item1)
                        for items in temp:
                            if items not in templist1:
                                templist2.append(items)
                        for item1 in templist1:
                            for items in templist1[templist1.index(item1)+1:]:
                                a = items.index(" AND")
                                b = items.index(" OR")
                                c = item1.index(" AND")
                                d = item1.index(" OR")
                                if ((item1[c+5:d-1] == items[1:a] or item1[c+5:d-1] == items[a+5:b-1]) and (item1[1:c] == items[1:a] or item1[1:c] == items[a+5:b-1])):
                                    if item1 not in templist2:
                                        if item1 != items:
                                            templist2.append(item1)    
                        temp_sql_11 = templist2[ :-1]
                        v1 = templist2[-1][:-3]
                        temp_sql_11.append(v1)
                        hv = formatted_sql.index('having')
                        templist3 = formatted_sql[p:hv]
                        for item in templist3:
                            if item not in templist4:
                                templist4.append(item)
                        last = templist4[-1][:-1]
                        temp_sql_111 = templist4[:-1]
                        temp_sql_111.append(last)
                        templist5 = formatted_sql[hv:]
                        for item in templist5:
                            if item not in templist6:
                                templist6.append(item)
                        temp_sql_1111 = templist6[:-2]
                        v3 = templist6[-2][:-3]
                        temp_sql_1111.append(v3)        
                        for_sql = temp_sql +temp_sql_1+temp_sql_11+temp_sql_111+temp_sql_1111
                else:
                    templist = formatted_sql[wh+1:-1]
    
                    for item in templist:
                        if "AND" in item:
                            if item not in temp:
                                temp.append(item)
                    for item in templist:
                        if item not in templist1:
                            if "AND" not in item:
                                templist2.append(item)   
                    for item1 in temp:
                        for items in temp:
                            a = items.index(" AND")
                            b = items.index(" OR")
                            c = item1.index(" AND")
                            d = item1.index(" OR")
                            if ((item1[c+5:d-1] == items[1:a] or item1[c+5:d-1] == items[a+5:b-1]) and (item1[1:c] == items[1:a] or item1[1:c] == items[a+5:b-1])):
                                if item1 != items:
                                    if item1 not in templist1:
                                        templist1.append(item1)
                    for items in temp:
                        if items not in templist1:
                            templist2.append(items)
                    for item1 in templist1:
                        for items in templist1[templist1.index(item1)+1:]:
                            a = items.index(" AND")
                            b = items.index(" OR")
                            c = item1.index(" AND")
                            d = item1.index(" OR")
                            if ((item1[c+5:d-1] == items[1:a] or item1[c+5:d-1] == items[a+5:b-1]) and (item1[1:c] == items[1:a] or item1[1:c] == items[a+5:b-1])):
                                if item1 not in templist2:
                                    if item1 != items:
                                        templist2.append(item1)       
                    temp_sql_11 = templist2[ :-2]
                    v1 = templist2[-1][:-3]
                    temp_sql_11.append(v1)  
                    for_sql = temp_sql +temp_sql_1+temp_sql_11
            
        
        elif val == 2:
                gp = formatted_sql.index('group by')
                if "having" in formatted_sql:
                    hv = formatted_sql.index('having')
                    temp_sql_1 = formatted_sql[fr1:gp]
                    templist = formatted_sql[gp:hv]
                    for item in templist:
                        if item not in templist1:
                            templist1.append(item)
                    temp_sql_11 = templist1[ :-1]
                    v2 = templist1[-1][:-1]
                    temp_sql_11.append(v2)
                    templist2 = formatted_sql[hv:-2]
                    v3 = formatted_sql[-2][:-3]
                    templist2.append(v3)
                    for_sql = temp_sql +temp_sql_1+temp_sql_11+templist2
                else:
                    temp_sql_1 = formatted_sql[fr1:gp]
                    templist = formatted_sql[gp:]
                    for item in templist:
                        if item not in templist1:
                            templist1.append(item)
                    temp_sql_11 = templist1[ :-2]
                    v3 = formatted_sql[-2][:-1]
                    templist2.append(v3)
                    for_sql = temp_sql +temp_sql_1+temp_sql_11+templist2
                    
        else:
                temp_sql_1 = formatted_sql[fr1:]
                for_sql = temp_sql +temp_sql_1[:-1]
    already_seen = set()
    for out_sql_sql in for_sql:
        if out_sql_sql.strip() not in already_seen:
            print(out_sql_sql)
            already_seen.add(out_sql_sql.strip())
            f_out.write(out_sql_sql+ "\n")
            
    logging.debug(out_sql_sql)

if __name__ == '__main__':
    start_time = datetime.now()
    this_function_name = sys._getframe(  ).f_code.co_name
    TimeStamp = time.strftime('%Y%m%d_%H%M%S')
    filename1=sys.argv[1].split('.')[0]+'_'+TimeStamp+'.log'
    logging.basicConfig(filename = filename1 , filemode='w',
                    format='%(asctime)s -   %(funcName)20s()   - %(levelname)s     -%(message)s', level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    excelInput = sys.argv[1]
    path1 = sys.argv[2]
    sheet_3 = pd.read_excel(excelInput, sheet_name=0, header = 1)
    check =''
    df = pd.read_excel(excelInput,index_col = None,header = None)
    base_table = df.iloc[0][1]
    df1 = sheet_3
    target_table = df1['TARGET_TABLE'][0]
    key_Words = ['LEFT','RIGHT','INNER','JOIN','ONE-2-ONE', 'CASE', 'DERIVED']
    string_functions = ['CONCAT', 'IFNULL', 'SUBSTR', 'TRIM','LTRIM','RTRIM','ISNULL','ISNOTNULL','LENGTH','UPPER','LOWER','LOCATE','REPLACE','SPLIT','CAST','MD','ARRAY','COALESCE','LPAD','RPAD','CEIL', 'ROUND']
    maths_list = ['*', '+', '-', '/']
    date_list = ['CURRENTDATE', 'DATEFORMAT', 'CURRENTTIMESTAMP', 'DATEDIFFERENCE','DATEADD','DATESUB','MONTHSBET','TODATE','EXTYEAR','EXTMONTH']
    aggregate_list = ['COUNT','SUM','AVG','MAX','MIN']
    lists = ['CASE']
    ranking= ['ROWNUMBER','RANK','DENSERANK']
    col = []
    output_sql = []
    formatted_sql = []
    val = []
    table_name1 = []        
    table_name = []
    temp_col = []
    x=[]
    y=[]
    grp =[]
    source_col=[]
    row1 = []
    row2 = []
    SQLoutput = sys.argv[1].split('.')[0]+".sql"
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, SQLoutput)
    try:
            f_out = open(filename, 'w')
            logging.info('opened file %s',filename)
    except IOError:
            logging.error('Could not open the file %s',filename)
            raise
    file_out = open("f_file.txt", 'w')
    file_out.write("---STAT---\n")
    for i in range(len(df1['TRANSFORMATION'])):
        row1.append(df1.loc[i,'TRANSFORMATION'])
    for index, row in df1.iterrows():
        row2.append(row)
        val = row['TRANSFORMATION']
        etl_rule(index, row,row1[index],file_out,val)
        table_name1.append(df1['SOURCE_TABLE'])
        source_col.append(df1['SOURCE_COLUMN'])
        y.append(df1['FILTER'].fillna('NA'))
    for i in range(len(y)):   
        x.append(y[0][i])
        table_name.append(table_name1[0][i])
    format_query(output_sql,temp_col,table_name,x,source_col,grp,row2)
    file_write(f_out)
    try:
        f_out.close()
        logging.info('closed file %s',filename)
    except IOError:
        print("Cant close file")    
        logging.error('Could not close the file %s',filename)
    end_time = datetime.now()
    time_elapsed = datetime.now() - start_time
    hdfsCreate = 'hadoop fs -mkdir -p '+path1
    subprocess.call([hdfsCreate], shell=True)
    hdfsPut = 'hadoop fs -put '+SQLoutput+' '+path1+SQLoutput.split('/')[len(SQLoutput.split('/'))-1]
    subprocess.call([hdfsPut], shell=True)
    logging.info('hdfs file location : '+path1+SQLoutput.split('/')[len(SQLoutput.split('/'))-1])
    print(':'+SQLoutput.split('/')[len(SQLoutput.split('/'))-1]+':'+target_table)
    logging.info('total time elapsed : {}'.format(time_elapsed))
    