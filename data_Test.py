import datetime
import matplotlib.pyplot as plt
import pandas as pd
import os

def ex1(csv_file):
    #find all elements with PW_availabecapacity > 3300
    #convert unix time to real time -> sum all time by monthly
    #plot chart
    df = pd.read_csv(csv_file)
    #print(df.head())

    signal_of_intr = "PW_AvailableChargePower"

    #convert unix timestamp to date time format
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms').apply(lambda x: x.to_datetime64())

    #extract month from string
    df['month'] = pd.DatetimeIndex(df['time']).month
    df['year'] = pd.DatetimeIndex(df['time']).year

    #print(df.head())

    #filter for only PW_AvailableChargePower and > 3300
    avail_mask = (df['signal_name']== signal_of_intr) & (df['signal_value'] >= 3300)
    avail_pw_df =df.loc[avail_mask]
    #print(len(avail_pw_df.index))

    #look at each months average number of records 
    
    #store the date mask for easy filtering on months and years to compute averages
    unique_months = avail_pw_df['month'].unique()
    unique_years = avail_pw_df['year'].unique()
    #print(unique_months)
    #print(unique_years)

    df_by_avg_month= []
    PW_AVG_BY_MO = []

    #store x_tick label for plotting purposes
    x_tick_label =[]

    #use datemask month and year to filter dataframe into n number of month-year dateframes
    for year in unique_years:
        for month in unique_months:

            #append pd.dateframe if month and date match, therefore we have n copies of month,year dateframes
            date_mask = (avail_pw_df['month'] == month) & (avail_pw_df['year'] == year)
            df_by_avg_month.append(avail_pw_df.loc[date_mask])

            #store x_tick label for plotting Month-Year 
            x_tick_label.append(datetime.date(year,month,1).strftime("%b %y"))
    
    #print(df_by_avg_month)

    # used to store month in order by year ex) [7,8,9,10,11,12,1,2, ...]
    x_label = [] 
    for index in range(0,len(df_by_avg_month)):
        #compute average avaialble charge power by month and year
        x_label.append(df_by_avg_month[index]['month'].iloc[0])
        PW_AVG_BY_MO.append(df_by_avg_month[index]['signal_value'].sum()/len(df_by_avg_month[index]))

    title = "Battery %s With SOE >90%%" %(csv_file[-7:-4])

    #return params for subplot outside function
    return (x_label,x_tick_label,PW_AVG_BY_MO,title)

def find_missing_indicies(df,signal_capacity,signal_remaining, signal_of_intr):
    """iterate over column sigval and identify missing data indices.
    used indices for padding with 0s later.
    """

    missing_data_indicies = []
    group_array = []

    #used to keep track of clusters that are missing data
    group = 0
    for index in range(0,len(df['signal_value'])):

        prev_min = " "

        if (index > 0):
            #record previous unix timestamp
            prev_min = df['timestamp'].iloc[index-1]
        
            #record current unix timestamp
            current_min = df['timestamp'].iloc[index]

            #record difference between unix timestamps, condition for 5 minutes
            delta_time = (current_min - prev_min)/1000/6

            group_array.append(group)

            #is current PW_FullPackEnergyAvailable ?
            if (df['signal_name'].iloc[index] ==  signal_capacity):

                #is previous field "PW_AvailableChargePower"?
                 if (df['signal_name'].iloc[index-1] != signal_of_intr):
                    
                    #record meta information for previous index to use to pad array
                    missing_group = (group-1,df['Primary_key'].iloc[index-1],signal_of_intr,df['battery_serial'].iloc[index-1],df['timestamp'].iloc[index-1],None)
                    missing_data_indicies.append(missing_group)

                    #print(delta_time,df['signal_name'].iloc[index], df['signal_name'].iloc[index-1],group)

                    #update group if "PW_AvailableChargePower"from previous cluster is gone
                    group += 1

                    #remove previous element with wrong group id
                    group_array.pop(group)

                    #append updated group id
                    group_array.append(group)


            #is current field "PW_EnergyRemaining"?
            elif (df['signal_name'].iloc[index] ==  signal_remaining): 
                #is previous PW_FullPackEnergyAvailable ?
                if(df['signal_name'].iloc[index-1] != signal_capacity):
                    
                    missing_group = (group,df['Primary_key'].iloc[index],signal_capacity,df['battery_serial'].iloc[index],df['timestamp'].iloc[index],None)
                    missing_data_indicies.append(missing_group)
                    #print(delta_time,df['signal_name'].iloc[index], df['signal_name'].iloc[index-1],group)

            #is current field "PW_AvailableChargePower"?
            elif (df['signal_name'].iloc[index] == signal_of_intr):
                
                #is previous field " PW_EnergyRemaining 
                if (df['signal_name'].iloc[index-1] != signal_remaining):
                   
                    missing_group = (group,df['Primary_key'].iloc[index], signal_remaining,df['battery_serial'].iloc[index],df['timestamp'].iloc[index],None)
                    missing_data_indicies.append(missing_group)
                    #print(delta_time,df['signal_name'].iloc[index], df['signal_name'].iloc[index-1],group)

                #update group when we reach "PW_AvailableChargePower" for next cluster
                group +=1

        else:
            #is current PW_FullPackEnergyAvailable ?
            if (df['signal_name'].iloc[index] !=  signal_capacity):
                    
                    missing_group = (group,df['Primary_key'].iloc[index], signal_capacity,df['battery_serial'].iloc[index],df['timestamp'].iloc[index],None)
                    missing_data_indicies.append(missing_group)

            group_array.append(group)
                
    df['Group'] = group_array
    #print(df.head())
    #print(len(missing_data_indicies))
    #print(missing_data_indicies)
    #print(len(group_array))

    #print("# of unique groups: " ,df['Group'].unique())

    return (missing_data_indicies,df)

def pad_df(df,missing_data_index_array):
    """
    pad dataframe with None for missing data
    """

    #used to keep track of number of additions to dataframe
    count_num_inserts = 0
    #iterate over missing data index array
    for index in range(0,len(missing_data_index_array)):

        group = missing_data_index_array[index][0]
        Pk = missing_data_index_array[index][1]
        sig_name = missing_data_index_array[index][2]
        bat_id = missing_data_index_array[index][3]
        ts = missing_data_index_array[index][4]
        sig_val = missing_data_index_array[index][5]

        #add offset as array grows
        ins_pos = Pk + count_num_inserts 

        line1 = pd.DataFrame({"battery_serial": bat_id, "timestamp": ts,  "signal_name": sig_name,"signal_value" : sig_val, "Primary_key" : Pk, "Group": group}, index=[ins_pos])
        df = pd.concat( [ df.iloc[:ins_pos],line1, df.iloc[ins_pos:]] ).reset_index(drop=True)

        count_num_inserts += 1

    return df

def data_row_check(df):
    """Finds rows with missing  data and filter output""" 

    row_mask =  pd.notnull(df['signal_value']) | pd.notnull(df['signal_name'])  | pd.notnull(df['timestamp'])

    #print(row_mask)

    return df[row_mask]

def ex2(csv_file):
    #compute new column SOE: PW_EnergyRemaining / PW_FullPackEnergyAvailable
    #filter out SOE > 90%, for PW_AvailableChargePower

    df = pd.read_csv(csv_file)
    #print(df.head())
    signal_of_intr = "PW_AvailableChargePower"
    signal_capacity = "PW_FullPackEnergyAvailable"
    signal_remaining = "PW_EnergyRemaining"

    #insert row num to keep track of indices
    df.insert(0, 'Primary_key', range(0,len(df)))  # here you insert the row count


    #print(df['signal_name'].value_counts())

    missing_indexes, df_with_group  = find_missing_indicies(df,signal_capacity,signal_remaining,signal_of_intr)

    df = pad_df(df_with_group,missing_indexes)

    df = data_row_check(df)

    #print(len(df))

    #convert unix timestamp to date time format
    df['time'] = pd.to_datetime(df['timestamp'], unit='ms').apply(lambda x: x.to_datetime64())


    #extract month from string
    df['month'] = pd.DatetimeIndex(df['time']).month
    df['year'] = pd.DatetimeIndex(df['time']).year
    #print(df['month'].value_counts())
    #print(df['year'].value_counts())
    #print(df.head())

    # filter 3 separate signal names
    avail_mask = (df['signal_name']== signal_of_intr)
    avail_mask_capacity = (df['signal_name']== signal_capacity)
    avail_mask_remaining = (df['signal_name'] ==signal_remaining)

    #print("-----------")
    #print("signal_names_split")
    #print(df['signal_name'].value_counts())
    #print("-----------")

    avail_pw_df =df.loc[avail_mask].copy()
    avail_pw_df.insert(0, 'split_key', range(0,len(avail_pw_df)))  # here you insert a new row count from 0 to len of avail_pw_df
    avail_capacity_df = df.loc[avail_mask_capacity].copy()
    avail_capacity_df.insert(0, 'split_key', range(0,len(avail_capacity_df)))  # here you insert a new row count from 0 to len of avail_capacity_df
    avail_remaining_df = df.loc[avail_mask_remaining].copy()
    avail_remaining_df.insert(0, 'split_key', range(0,len(avail_remaining_df)))  # here you insert a new row count from 0 to len of avail_remaining_df


    #calculate SOE, PW_EnergyRemaining / PW_FullPackEnergyAvailable
    SOE_list = []

    #print(len(avail_pw_df),len(avail_capacity_df), len(avail_remaining_df))

    for index in range(0,len(avail_capacity_df)):
      soe = avail_remaining_df['signal_value'].iloc[index]/avail_capacity_df['signal_value'].iloc[index]
      SOE_list.append(soe)

    #print(SOE_list[3])
    #print("---",len(SOE_list),len(avail_pw_df),len(avail_capacity_df), len(avail_remaining_df))

    avail_pw_df['SOE'] = SOE_list
    avail_pw_max_mask = (avail_pw_df['signal_value'] >= 3300)  & (avail_pw_df['SOE'] <=0.9)
    avail_pw_df = avail_pw_df[avail_pw_max_mask]
    #print(len(avail_pw_df))

    #print('-------------------------')
    #print("AvailChargePower: ")
    #print(avail_pw_df['signal_value'].value_counts())
    #print("\n")
    #print(avail_pw_df['month'].value_counts())
    #print("\n")
    #print(avail_pw_df['year'].value_counts())
    #print('--------------------------')
    #store the date mask for easy filtering on months and years to compute averages
    unique_months = avail_pw_df['month'].unique()
    unique_years = avail_pw_df['year'].unique()
    #print(unique_months)
    #print(unique_years)

    df_by_avg_month= []
    PW_AVG_BY_MO = []

    #store x_tick label for plotting purposes Mon - Year
    x_tick_label =[]

    #use datemask month and year to filter dataframe into n number of month-year dateframes
    for year in unique_years:
        for month in unique_months:

            #append pd.dateframe if month and date match, therefore we have n copies of month,year dateframes
            date_mask = (avail_pw_df['month'] == month) & (avail_pw_df['year'] == year)
            df_by_avg_month.append(avail_pw_df.loc[date_mask])

            #store x_tick label for plotting Month-Year 
            x_tick_label.append(datetime.date(year,month,1).strftime("%b %y"))
    

    # used to store month ex) [7,8,9,10,11,12,1,2, ...]
    x_label = [] 
    for index in range(0,len(df_by_avg_month)):
        #compute average avaialble charge power by month and year
        x_label.append(df_by_avg_month[index]['month'].iloc[0])
        PW_AVG_BY_MO.append(df_by_avg_month[index]['signal_value'].sum()/len(df_by_avg_month[index]))

    title = "Battery %s Without SOE >90%% " %(csv_file[-7:-4])

    return (x_label,x_tick_label,PW_AVG_BY_MO,title)
    

def ex3(directory):
    #pass 5 csv files
    #plot 6 subplots for 5 files, 5 for exercise three and 1 for excercise one
    
    list_dir = os.listdir(directory)
    #print(list_dir)

    nrows = 2
    ncols = 3

    f, axes= plt.subplots(nrows,ncols)

    #extract plot parameters
    list_of_plots =[]
    for file in list_dir:
        filepath = os.path.join(directory,file)
        try:
            #if 1st csv file, append 2 copies: one using ex1(), one using ex2()
            if('001.csv' in filepath):
                list_of_plots.append(ex1(filepath))

            #for each battery file, save x and y axis 
            list_of_plots.append(ex2(filepath))
            #print(filepath)
        except:
            print("not a valid file")

    #plot subplots
    index = 1
    for row in range(0,nrows):
        
        for col in range(0,ncols):

            #if we reach the last row,col figure
            if ((index > len(list_of_plots)) or (index ==6)):
                break

            x_label =list_of_plots[index][0]
            x_tick_label =list_of_plots[index][1]
            avg_charge = list_of_plots[index][2]
            title = list_of_plots[index][3]

            #print(len(avg_charge), " ", len(x_label))
            
            #if we have one average computed, plot  mark type
            if (len(avg_charge) < 2):
                axes[row][col].plot(x_label,avg_charge,'o', color= 'red' ,markersize=14)
            else:
                axes[row][col].plot(x_label,avg_charge,color='red')

            # Set the tick positions
            axes[row][col].set_xticks(x_label)

            # Set the tick labels
            axes[row][col].set_xticklabels(x_tick_label)
            axes[row][col].set_title(title ,fontsize=14)
            axes[row][col].set_xlabel('Month',fontsize=14)
            axes[row][col].set_ylabel('Avg ChargePower (Wh)',fontsize=14)
            axes[row][col].autoscale(enable=True, axis='both',tight=None)

            #account for number of figurs plotted
            index+=1

    #plot first battery with SOE > 90%
    x_label =list_of_plots[0][0]
    x_tick_label =list_of_plots[0][1]
    avg_charge = list_of_plots[0][2]
    title = list_of_plots[0][3]

    axes[nrows-1][ncols-1].plot(x_label,avg_charge)
    
    # Set the tick positions
    axes[nrows-1][ncols-1].set_xticks(x_label)

    # Set the tick labels
    axes[nrows-1][ncols-1].set_xticklabels(x_tick_label)

    axes[nrows-1][ncols-1].set_title(title ,fontsize=14)
    axes[nrows-1][ncols-1].set_xlabel('Month',fontsize=14)
    axes[nrows-1][ncols-1].set_ylabel('Avg ChargePower (Wh)',fontsize=14)
    axes[nrows-1][ncols-1].autoscale(enable=True, axis='both',tight=None)

    
    plt.show()

def test2():
    #example of adding new rows to dataframe while changing size of dataframe
    d = {'one':[1,2,3,4,5,6,7], 'two':[1,2,3,4,5,6,7], 'three':[1,2,3,4,5,6,7]}
    line1 = pd.DataFrame({"one": 30, "two": 30,  "three": 30}, index=[2])
    line2 = pd.DataFrame({"one": 60, "two": 60,  "three": 60}, index=[4])
    d = pd.DataFrame(d)
    
    first_pos = 2
    count_num_adds = 0

    d = pd.concat( [ d.iloc[:first_pos], line1, d.iloc[first_pos:]] ).reset_index(drop=True)
    print(d)
    count_num_adds+=1

    pos = 3 + count_num_adds

    d = pd.concat( [ d.iloc[:pos],line1, d.iloc[pos:]] ).reset_index(drop=True)
    print(d)
    

if __name__ =='__main__':

    directory = os.path.join(input("Please pass in path to data directory containing 5 csv files: "))
    ex3(directory)

