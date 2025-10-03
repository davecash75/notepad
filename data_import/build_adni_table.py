from pathlib import Path
import pandas as pd

data_root=Path("/Users/davecash/Library/CloudStorage/OneDrive-UniversityCollegeLondon/NOTEPAD/data/ADNI/NOTEPAD_ADNI_CU")
mytable_path = data_root / "NOTEPAD_ADNI_CU_My_Table_29Aug2025.csv"
mri_path = data_root / "NOTEPAD_ADNI_CU_Structural_MRI_Images_29Aug2025.csv"
pet_path = data_root / "NOTEPAD_ADNI_CU_PET_Images_29Aug2025.csv"
registry_path = data_root / "REGISTRY_29Aug2025.csv"

mr_keep_cols = [
            'image_id','subject_id','study_id','mri_visit',
            'mri_date','mri_description','mri_thickness',
            'mri_mfr','mri_mfr_model','mri_field_str'
            ]

pet_keep_cols = [
            'image_id','subject_id','study_id','pet_visit',
            'pet_date','pet_description','pet_mfr',
            'pet_mfr_model','pet_radiopharm'
            ]

df_myvars = pd.read_csv(mytable_path)
df_mri = pd.read_csv(mri_path)
df_mri = df_mri[mr_keep_cols]
df_mri = df_mri.rename(
            columns={'mri_visit': 'image_visit',
                    'mri_date': 'image_date',
                    'mri_description': 'image_description'}
            )   
df_pet = pd.read_csv(pet_path)
df_pet = df_pet[pet_keep_cols]
df_pet = df_pet.rename(
            columns={'pet_visit': 'image_visit',
                    'pet_date': 'image_date',
                    'pet_description': 'image_description'}
            )
        # Remove FDG and PIB (for time being)
df_pet = df_pet.loc[df_pet["pet_radiopharm"]!="11C-PIB"]

df_registry = pd.read_csv(registry_path)
df_registry = df_registry[
        ["PTID","RID","VISCODE","VISCODE2","EXAMDATE"]]
df_registry = df_registry.rename(
        columns={'PTID':'subject_id',
                 'VISCODE':'image_visit',
                 'VISCODE2':'visit'})

df_myvars_visitmap = pd.merge(df_myvars,df_registry,
                              on=['subject_id','visit'],
                              how='left')
print(df_myvars_visitmap)
print(df_myvars_visitmap.columns)


df_master_mri = pd.merge(df_myvars_visitmap,df_mri,
                     on=['subject_id','image_visit'],
                     how='left')

df_master_pet = pd.merge(df_myvars_visitmap,df_pet,
                     on=['subject_id','image_visit'],
                     how='left')

df_master_mri = df_master_mri.sort_values(by=['subject_id','EXAMDATE'])
df_master_pet = df_master_pet.sort_values(by=['subject_id','EXAMDATE'])
print(df_master.columns)
print(df_master)
df_master.to_csv(data_root / "master_test.csv")

