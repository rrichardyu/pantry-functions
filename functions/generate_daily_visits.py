import pandas as pd
import datetime
import json
import sys
import re

def generate_daily_visits(df, week_num):
    df

    df = df[df['Quantity'] != 4041570054161]
    df = df[df['Quantity'] != 4024182025064]
    df = df[df['Quantity'] != 2027862020168]

    df['datetime'] = pd.to_datetime(df['Date'], errors="coerce")
    df['datetime'] = df['datetime'].ffill()

    # Create a new column 'weekday' with the names of the weekdays
    df['weekday'] = df['datetime'].dt.day_name()
    df['timefilled'] = df['Time'].ffill()

    # Calculate the difference in days from the start date
    df['days_from_start'] = (df['datetime'] - df['datetime'].min()).dt.days

    # Calculate the week number starting from 0
    df['week #'] = df['days_from_start'] // 7

    # Drop the 'days_from_start' column as it's no longer needed
    df.drop('days_from_start', axis=1, inplace=True)

    # Add people # column
    counter = 0
    week_nums = []

    for date in df['Date']:
        if pd.notna(date):
            counter += 1
        week_nums.append(counter)

    df['people #'] = week_nums


    def cat_item(item_name):
        if item_name in ['Earthbound Organic Frozen Blueberries']:
            return 'Frozen Blueberries'
        elif item_name in ['Apple', "Apples"]:
            return 'Fresh Fruit'
        elif item_name in ['Fat Free Refried Beans', "Rosarita No-Fat Refried Beans, Canned", "Rosarita Zesty Salsa Refried Beans"]:
            return 'Refried Beans'
        elif item_name in ['original edensoy organic soymilk ', "original edensoy organic soymilk", "Eden Original Soy Milk", "Edensoy Organic Soymilk", "Edensoy Soy Milk", 'Silk Original Soy Milk', "original edensoy organic soymilk"]:
            return 'Soy Milk'
        elif item_name in ["Edensoy Organic Soymilk Unsweetened", "Edensoy Organic Soy Milk Unsweetened", 'West Life Unsweetened Soy Milk', "West Life Unsweetened Soymilk"]:
            return "Unsweetened Soy Milk"
        elif item_name in ['Sahara Apple Juice ', "Sahara Apple Juice", 'Sahara Apple Juice', "Totally Juice, 100% Apple Juice"]:
            return 'Apple Juice'
        elif item_name in ['Castle Chili with Beans', 'Southgate Chicken Chili with Beans']:
            return 'Chili'
        elif item_name in ["Robert's White Rice", "Roberts White Rice",  "basmati rice", "FlickerTail Enriched Long Grain Rice"]:
            return 'White Rice'
        elif item_name in ['Dry Black Beans', "Low Sodium Black Beans", "Field Day Black Beans", "Eden Organic Black Beans"]:
            return 'Black Beans'
        elif item_name in ["Hunt's Pasta Sauce ", "Hunt's Pasta Sauce", "Pasta Sauce, Mc Trader 15 oz", "Whole Wheat Spaghetti", 'Star Cross Spaghetti Sauce', "Pasta Sauce, Mc Trader 15oz"]:
            return "Pasta Sauce"
        elif item_name in ["Green Lentils", 'field day organic lentils', "Field Day Organic Lentils", "Field Day Organic Red Lentils", "Celia's Lentils", "Red Lentils", "Farmer Direct Red Lentils"]:
            return 'Lentils'
        elif item_name in ['Organic Seaweed Snacks ', "Annie Chun's Organic Seaweed Snacks", "Organic Seaweed Snacks", "Organic Seaweed Snacks"]:
            return 'Seaweed Snacks'
        elif item_name in ["Maeploy Sweet Chili Sauce"]:
            return 'Chili Sauce'
        elif item_name in ["Yamasa Shoyu Soy Sauce", "Yamasa Soy Sauce", "Kikkoman Soy Sauce", "Kikkoman Gluten-Free Soy Sauce", "Tamari Soy Sauce", "Kikkoman Soy Sauce (09/22/2023)"]:
            return "Soy Sauce"
        elif item_name in ["quick oats", "Avelina Rolled Oats", "Quick Oats", "Vanilla Honey Oats"]:
            return "Oats"
        elif item_name in ["Dynasty Sesame oil", "Dynasty Sesame Oil", "Kikkoman Sesame Oil", "Sesame oil"]:
            return "Sesame Oil"
        elif item_name in ["Celia's Pinto Bans ", "Celia's Pinto Beans ", "Celia's Pinto Bans", "Celia's Pinto Beans", "Cojack Pinto Beans", "Jack's Superior Triple Cleaned Pinto Beans", "Celia's Pinto Bans", "Nature Valley Organic Pinto Beans", "Jack's Superior Pinto Beans", "Celia's Dry Pinto Beans", "Dry Pinto Beans"]:
            return "Pinto Beans"
        elif item_name in ["Northern Pride Young Turkey"]:
            return "Turkey"
        elif item_name in ["Minor Figures Barista Oat Milk (Organic)", "oat milk", "Minor Figure Barista Oat Milk"]:
            return "Oak Milk"
        elif item_name in ["Great Choice Mixed Vegetables"]:
            return "Mixed Vegetables"
        elif item_name in ["Almond Original, Unsweetened Almond Milk"]:
            return "Unsweetened Almond Milk"
        elif item_name in ["Almond Breeze Almond Milk Original", "Almond Breeze Almond Milk Vanilla", "Almond Breeze Almond Milk"]:
            return "Almond Milk"
        elif item_name in ["Field Day Turmeric"]:
            return "Turmeric"
        elif item_name in ["Tyson Whole Chicken", "Chicken Drumsticks", "Crider Premium White Chicken", "mercantile&fancy Chunk White Chicken", "Chicken Thighs", "Amick Farms Chicken Drumsticks (10/6)", "Chicken Drumstick", "Crider Canned Dark Chicken", "Amick Farms Chicken Drumsticks"]:
            return "Chicken"
        elif item_name in ["Halal Chicken Drumsticks"]:
            return 'Halal Chicken'
        elif item_name in ["Santa Sophia Whole Wheat Spaghetti", "Allegra Spaghetti", "Bionat WW Spaghetti", "Field Day Organic Spaghetti", "Doria Spaghetti", "Field Day Trad Spaghetti", "Whol e Wheat Spaghetti"]:
            return "Spaghetti"
        elif item_name in ["Dairy Pure 8 oz 1% Milk", "Tiny Dairy Pure Milk 1% (1/2 Pint)", "alpura 2% reduced fat milk", "Dairy Pure Lowfat Milk"]:
            return "Milk"
        elif item_name in ['Salvatore Di Caro Ex Vir Olive Oil ', "Salvatore Di Caro Ex Vir Olive Oil", "califirnia olive ranch extra virgin olive oil", "Extra Virgin Olive Oil", "Field Day Extra Virgin Olive Oil", "Santa Barbara Extra Virgin Olive Oil", "Salvatore Di Caro Ex Vir Olive Oil"]:
            return "Olive Oil"
        elif item_name in ['H.E.B Diced Pears in Organic Fruit Juices', 'Diced Pears in 100% juice', "Diced Pears (4 cups)", "Clearly Diced Pears", "Tara Diced Peaches In Natural Juice", "365 Organic Diced Pears"]:
            return "Diced Pears"
        elif item_name in ['Mixed Green Beans', "Stokely's Cut Green Beans", "Early Garden Mixed Green Beans", "Cut Green Beans"]:
            return "Green Beans"
        elif item_name in ["Diced Tomatoes 10/23", "California Healthy Harvest: Diced Tomatoes", "Summer is Inside Diced Tomatoes", "Pomi Chopped Tomatoes"]:
            return "Diced Tomatoes"
        elif item_name in ["Extra Firm Tofu"]:
            return"Firm Tofu"
        elif item_name in ['California Healthy Harvest Sliced Peaches', 'orchard natural organic sliced peaches ', "Orchard Naturals Sliced Peaches", "Canned Peach Slices", "orchard natural organic sliced peaches", "Chef Maxwell Sliced Peaches", "Orchard Naturals Organic Slices Peaches, Canned", "orchard natural organic sliced peaches", "Dole diced peaches cup"]:
            return "Canned Peaches"
        elif item_name in ["La Tourangelle Canola Oil Cooking Spray", "Field Day OG2 Evoo Cooking Spray", "La Tourangelle Cooking Spray", "Field Day Organic Extra Virgin Olive Oil Cooking Spray"]:
            return "Cooking Spray"
        elif item_name in ["Fruit Mix, canned"]:
            return "Canned Fruit"
        elif item_name in ["Earthbound Frozen Strawberries", "Organic Strawberries (frozen)"]:
            return "Frozen Strawberries"
        elif item_name in ["Sliced Bamboo Shoots", "Duchess Sliced Bamboo Shoots"]:
            return "Bamboo Shoots"
        elif item_name in ["Eden Organic Garbanzo Beans", "Field Day Garbanzo Beans Canned", "Field Day Garbanzo Beans", "Garbanzo Beans (Canned)", "Garbanzo Beans (Dry)", "Celia's Garbanzo Beans (Dry)"]:
            return "Garbanzo Beans"
        elif item_name in ["Celia's Light Red Kidney Beans"]:
            return "Kidney Beans"
        elif item_name in ["Frozen Chicken, ground"]:
            return "Ground Chicken"
        elif item_name in ["Creamy & Unsalted Peanut Butter", "Creamy and Unsalted Peanut Butter", "Duchess Peanut Butter"]:
            return "Peanut Butter"
        elif item_name in ["Fisha Tuna Chunk Light Tuna in Water", "Duchess Chunk Light Tuna"]:
            return "Canned Tuna"
        elif item_name in ["Mixed Vegetable", "Peas and Carrots", "Woodstock Organic Frozen Mixed Vegetables", "Chef Maxwell Mixed Vegetables", "Stahlbush Classic Frozen Mixed Veggies"]:
            return "Mixed Vegetables"
        elif item_name in ["Riceland Brown Rice"]: 
            return "Brown Rice"
        elif item_name in ["Wyman's Mango Chunks", "Earthbound Mango Chunks"]: 
            return "Mango Chunks"
        elif item_name in ["ground beef"]:
            return "Ground Beef"
        elif item_name in ["Premier Pantry Macaroni & Cheese", "Macaroni & Cheddar Dinner (Signature)"]:
            return "Macaroni and Cheese"
        elif item_name in ["Totally Juice Fruit Punch", "Totally Juice, 100% Fruit Punch Juice", "Totally Juice, 100% Fruit Juice Box", "Fruit Punch Juice", "Totally Juice, 100% Berry Juice"]:
            return "Juice Box"
        elif item_name in ['Vietnamese Rice Vermicelli ', "Noodles (Vietnamese Rice Vermicelli)", "Vietnamese Rice Vermicelli"]:
            return "Rice Noodles"
        elif item_name in ["Better Than Bouillion Roasted Chicken Base (Reduced Sodium)", "Better Than Bouillon Roasted Chicken Base"]:
            return "Chicken Bouillion"
        elif item_name in ["Frozen Tilapia Filet (fish)", "Tilapia Fillet"]:
            return "Tilapia Fillet"
        elif item_name in ['Earthbound Farm Organic Blueberries ', 'Blueberries Stahlbush island farm ', "Blueberries Stahlbush island farm", "Blueberries Stahlbush island farm", "Earthbound Farm Organic Blueberries"]:
            return "Blueberries"
        elif item_name in ["Carolina Ground Turkey", "Northern Pride Ground Turkey"]:
            return "Ground Turkey"
        elif item_name in ["Earthbound Farm Organic Green Peas"]:
            return "Peas"
        elif item_name in ['Better than Bouillon Vegan No Beef Base', "Better Than Bouillon Vegetable Base", "BTB vegan no chicken base", "Better than Bouillon Sauteed Onion Base", "Better Than Bouillon Seasoned Vegetable Base (Reduced Sodium)", "Better Than Bouillon Vegetable"]:
            return "Vegetable Bouillon"
        elif item_name in ["Celebrity Pork Luncheon Meat", "Pork Luncheon Meat"]:
            return "Pork Luncheon Meet"
        elif item_name in ["Vanilla Cereal", "Honey Bunches of Oats Vanilla Cereal", "Scooters cereal", "Scooters Cereal Bowls (Plain)", "Honey Scooters Cereal", "honey bunches of oats", "Honey Scooters Cereal Bowls", "Scooters Cereal Bowls (Honey)", "Marshmallow Mateys Cereal"]:
            return "Cereal"
        elif item_name in ['Whole Kernel Corn', "Signature Whole Kernel Corn", "North Pride Whole Kernel Corn", "Chef Maxwell Sweet Corn"]:
            return "Corn"
        elif item_name in ["Farmer's Market Organic Pumpkin Puree"]:
            return "Pumpkin Puree"
        elif item_name in ["Teasdale White Hominy (Maiz Blanco)", "Goya White Hominy"]:
            return "White Hominy"
        elif item_name in ["Del Monte Lite Mixed Fruit", "Dole Mixed Fruit Cup", "Dole Pineapple Fruit Cups", "Mixed Cherry Fruit Cups", "Fruit Mix in Juice", "Dole Pineapple Cups"]:
            return "Fruit Cup"
        elif item_name in ["Chef Boyardee Beef Ravioli, Canned"]:
            return "Beef Ravioli"
        elif item_name in ["Sliced Water Chestnuts", "Duchess Sliced Water Chestnuts"]:
            return "Water Chestnuts"
        elif item_name in [' Field Day Cayenne Pepper', "Field Day Cayenne Pepper"]:
            return "Cayenne Pepper"
        elif item_name in ["Stahlbush Butternut Squash", "Earthbound Organic Butternut Squash"]:
            return "Butternut Squash"
        elif item_name in ["Sysco Classic Salt", 'Sysco  Classic Salt']:
            return "Salt"
        elif item_name in ['Field Day Saigon Cinnamon', "Spicely Cinnamon Ground"]:
            return "Cinnamon"
        elif item_name in ['Earthbound Organic Mashed Cauliflower']:
            return "Cauliflower"
        elif item_name in ["Simpli Red Quinoa", 'Simpli Tri-Color Quinoa', "Field Day organic quinoa"]:
            return "Quinoa"
        elif item_name in ['Green Acres Fruit Mix', 'Canned Fruit Mix']:
            return "Mixed Fruit"
        elif item_name in ["Castle Canning Chili with Beans"]:
            return 'Chili'
        elif item_name in ["pork loin", "pork chops"]:
            return "Pork"
        elif item_name in ["Premium Bouillon"]:
            return "Bouillon"
        elif item_name in ["Organic Rice Vinegar", "Marukan Rice Vinegar"]:
            return 'Rice Vinegar'
        elif item_name in ["Frozen Salmon"]:
            return "Salmon"
        else:
            return item_name

    df['Item Name'] = df['Item Name'].apply(cat_item)
    df.sort_values("week #", ascending=False)

    #how many items were checked out per week
    weekly_sum = df.groupby('week #')['Quantity'].sum()

    grouped_data = df.groupby('Item Name')['Quantity'].sum().to_frame().sort_values("Quantity", ascending=False).head(10)

    def weekly_frequency(week_num):
        maxval = max(df['week #']) - 1
        valid_weeks = []
        total_distr = []
        for i in range(week_num):
            valid_weeks = valid_weeks + [maxval - i]

        for i in range(len(valid_weeks)):
            df1 = df[df['week #'] == valid_weeks[i]]
            df1 = df1[df1["Time"].notna()]
            weekly_distr1 = df1['weekday'].value_counts().sort_index()
            total_distr += [weekly_distr1]
        
        
        #average_distr = total_distr / week_num

        #return total_distr
        #average_distr = sum(total_distr) / week_num
        total_distr = pd.concat(total_distr, axis=1).fillna(0)

        # Calculate the average distribution across weeks
        average_distr = total_distr.mean(axis=1)
        #return average_distr
        #final = {{"Monday": , "Tue"}
        #for week in total_distr:

        
        # Define the custom order of the days of the week
        custom_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

        # Sort the 'average_distr' Series by the custom order
        average_distr = average_distr.reindex(custom_order)

        # Drop all rows with NaN values
        average_distr = average_distr.dropna()

        result = { str(k): v for k, v in average_distr.items() }
        result["title"] = f"Average Frequency of Food Pantry Visits for Last {str(week_num)} Weeks"
        return result
        
    return weekly_frequency(week_num)