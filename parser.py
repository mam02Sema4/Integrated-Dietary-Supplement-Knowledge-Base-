import pandas as pd
import json
import os


def load_data(data_folder):

    rel_results={}
    rel_sources={}
    source_part=[]
    current=0

    mrrel_file = os.path.join(data_folder,"MRREL.RRF")
    mrconso_file=os.path.join(data_folder,"MRCONSO.RRF")

    ###Load data and assign column types to avoid warings, and reanme for certain columns to avoid confusions
    mrrel_df = pd.read_csv(mrrel_file,sep="|")
    mrrel_df.rename(columns={'SAB':'REL_SOURCE'},inplace=True)

    mrconso_df=pd.read_csv(mrconso_file,sep="|",dtype={'CUI': 'string', 'AUI': 'string', 'STR': 'string', 'SAB': 'string', 'SCODE': 'string', 'ISPREF': 'string'})
    mrconso_df.rename(columns={'SAB':'ATOM_SOURCE'},inplace=True)

    ### Only include rows from UMLS and MEDDRA sources
    mrconso_rows=mrconso_df.loc[(mrconso_df['ATOM_SOURCE'] == 'UMLS') | (mrconso_df['ATOM_SOURCE'] == 'MEDDRA')]

    ### Loop every CUI1 from the MRREL file
    ids=mrrel_df.query('CUI1 in @mrconso_rows.CUI').drop_duplicates(subset=['CUI1'])
    total_records=len(ids['CUI1'])
    for item in ids['CUI1']:
      current+=1
      print("Progress:",str(current)+"/"+str(total_records))
      cui1=item.strip()
      mrrel_rows=mrrel_df.loc[(mrrel_df['CUI1'] == cui1)]

      ###get needed columns for later use
      mrrel_rows_subset=mrrel_rows[['CUI1','REL','CUI2','RUI','REL_SOURCE']] 
      mrconso_rows_subset=mrconso_rows[['CUI','ATOM_SOURCE','STR','SCODE','ISPREF']] 
 
      result=pd.merge(mrconso_rows_subset, mrrel_rows_subset, left_on='CUI', right_on='CUI2')
      result=result.drop_duplicates(subset=['CUI1','ATOM_SOURCE','REL','STR','SCODE','RUI','REL_SOURCE','ISPREF'])
 
      """ 
      generate part of JSON example  
      "has_adverse_effect_on": [
            {
                "name": "Renal and urinary disorders",
                "meddra": "MEDDRA:10038359",
                "source": [
                    {
                        "name": "iDisk",
                        "record": "DR1378597"
                    },
                    {
                        "name": "NMCD"
                    }
                ]
            },
      """
      for index, row in result.iterrows():
        cui=row['CUI1']
        atom_source=row['ATOM_SOURCE']
        rel=row['REL']
        t_str=row['STR']
        t_scode=row['SCODE']
        rui=row['RUI']
        rel_source=row['REL_SOURCE']
        is_pref=row['ISPREF']
   
        rel_sources= {'name':'iDisk', 'record':rui}
        rel_sources1= {'name':rel_source}
        source_part=[]
        source_part.append(rel_sources)
        source_part.append(rel_sources1)
        json_dict = {'name':t_str,atom_source.lower():atom_source+":"+t_scode,'source':source_part}
        rel_results.setdefault(rel,[]).append(json_dict)

  
      ###Every atom from cui1 has the same copy in the json file
      mrconso_rows_tmp=mrconso_rows.loc[(mrconso_rows['CUI'] == cui)].drop_duplicates(subset=['SCODE','STR','ATOM_SOURCE'])
      for index, row in mrconso_rows_tmp.iterrows():
        s_scode=row['SCODE']
        s_str=row['STR']
        sab=row['ATOM_SOURCE']
        _id=sab+":"+s_scode
        rec={"_id":_id,"name": s_str ,sab.lower(): sab+":"+s_scode}
        for rel,doc1 in rel_results.items():
           rec[rel]=doc1
        yield rec
      rel_results={}
   

