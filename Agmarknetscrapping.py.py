import requests
from bs4 import BeautifulSoup
import pandas as pd 
import time 
import schedule
from datetime import datetime
import os
 
# crop_list=['Absinthe', 'Ajwan', 'Alasande Gram', 'Almond(Badam)', 'Alsandikai', 'Amaranthus', 'Ambada Seed', 'Ambady/Mesta', 'Amla(Nelli Kai)', 'Amphophalus', 'Amranthas Red', 'Antawala', 'Anthorium', 'Apple', 'Apricot(Jardalu/Khumani)', 'Arecanut(Betelnut/Supari)', 'Arhar (Tur/Red Gram)(Whole)', 'Arhar Dal(Tur Dal)', 'Asalia', 'Asgand', 'Ashgourd', 'Ashoka', 'Ashwagandha', 'Asparagus', 'Astera', 'Atis', 'Avare Dal', 'Bael', 'Bajji chilli', 'Bajra(Pearl Millet/Cumbu)', 'Balekai', 'balsam', 'Bamboo', 'Banana', 'Banana - Green', 'Banana flower', 'Banana Leaf', 'Banana stem', 'Barley (Jau)', 'basil', 'Bay leaf (Tejpatta)', 'Beans', 'Beaten Rice', 'Beetroot', 'Behada', 'Bengal Gram Dal (Chana Dal)', 'Bengal Gram(Gram)(Whole)', 'Ber(Zizyphus/Borehannu)', 'Betal Leaves', 'Betelnuts', 'Bhindi(Ladies Finger)', 'Bhui Amlaya', 'Big Gram', 'Binoula', 'Bitter gourd', 'Black Gram (Urd Beans)(Whole)', 'Black Gram Dal (Urd Dal)', 'Black pepper', 'BOP', 'Borehannu', 'Bottle gourd', 'Brahmi', 'Bran', 'Bread Fruit', 'Brinjal', 'Brocoli', 'Broken Rice', 'Broomstick(Flower Broom)', 'Bull', 'Bullar', 'Bunch Beans', 'Butter', 'buttery', 'Cabbage', 'Calendula', 'Calf', 'Camel Hair', 'Cane', 'Capsicum', 'Cardamoms', 'Carnation', 'Carrot', 'Cashew Kernnel', 'Cashewnuts', 'Castor Oil', 'Castor Seed', 'Cauliflower', 'Chakotha', 'Chandrashoor', 'Chapparad Avare', 'Chennangi (Whole)', 'Chennangi Dal', 'Cherry', 'Chikoos(Sapota)', 'Chili Red', 'Chilly Capsicum', 'Chironji', 'Chow Chow', 'Chrysanthemum', 'Chrysanthemum(Loose)', 'Cinamon(Dalchini)', 'cineraria', 'Clarkia', 'Cloves', 'Cluster beans', 'Coca', 'Cock', 'Cocoa', 'Coconut', 'Coconut Oil', 'Coconut Seed', 'Coffee', 'Colacasia', 'Copra', 'Coriander(Leaves)', 'Corriander seed', 'Cossandra', 'Cotton', 'Cotton Seed', 'Cow', 'Cowpea (Lobia/Karamani)', 'Cowpea(Veg)', 'Cucumbar(Kheera)', 'Cummin Seed(Jeera)', 'Curry Leaf', 'Custard Apple (Sharifa)', 'Daila(Chandni)', 'Dal (Avare)', 'Dalda', 'Delha', 'Dhaincha', 'dhawai flowers', 'dianthus', 'Double Beans', 'Dragon fruit', 'dried mango', 'Drumstick', 'Dry Chillies', 'Dry Fodder', 'Dry Grapes', 'Duck', 'Duster Beans', 'Egg', 'Egypian Clover(Barseem)', 'Elephant Yam (Suran)', 'Field Pea', 'Fig(Anjura/Anjeer)', 'Firewood', 'Fish', 'Flax seeds', 'Flower Broom', 'Foxtail Millet(Navane)', 'French Beans (Frasbean)', 'Galgal(Lemon)', 'Gamphrena', 'Garlic', 'Ghee', 'Giloy', 'Gingelly Oil', 'Ginger(Dry)', 'Ginger(Green)', 'Gladiolus Bulb', 'Gladiolus Cut Flower', 'Glardia', 'Goat', 'Goat Hair', 'golden rod', 'Gond', 'Goose berry (Nellikkai)', 'Gram Raw(Chholia)', 'Gramflour', 'Grapes', 'Green Avare (W)', 'Green Chilli', 'Green Fodder', 'Green Gram (Moong)(Whole)', 'Green Gram Dal (Moong Dal)', 'Green Peas', 'Ground Nut Oil', 'Ground Nut Seed', 'Groundnut', 'Groundnut (Split)', 'Groundnut pods (raw)', 'Guar', 'Guar Seed(Cluster Beans Seed)', 'Guava', 'Gudmar', 'Guggal', 'gulli', 'Gur(Jaggery)', 'Gurellu', 'gypsophila', 'Haralekai', 'Harrah', 'He Buffalo', 'Heliconia species', 'Hen', 'Hippe Seed', 'Honey', 'Honge seed', 'Hybrid Cumbu', 'hydrangea', 'Indian Beans (Seam)', 'Indian Colza(Sarson)', 'Irish', 'Isabgul (Psyllium)', 'Jack Fruit', 'Jaee', 'Jaffri', 'Jaggery', 'Jamamkhan', 'Jamun(Narale Hannu)', 'Jarbara', 'Jasmine', 'Javi', 'Jowar(Sorghum)', 'Jute', 'Jute Seed', 'Kabuli Chana(Chickpeas-White)', 'Kacholam', 'Kakada', 'kakatan', 'Kalihari', 'Kalmegh', 'Kankambra', 'Karamani', 'karanja seeds', 'Karbuja(Musk Melon)', 'Kartali (Kantola)', 'Kevda', 'Kharif Mash', 'Khirni', 'Khoya', 'Kinnow', 'Knool Khol', 'Kodo Millet(Varagu)', 'kokum', 'Kooth', 'Kuchur', 'Kulthi(Horse Gram)', 'Kutki', 'kutki', 'Ladies Finger', 'Laha', 'Lak(Teora)', 'Leafy Vegetable', 'Lemon', 'Lentil (Masur)(Whole)', 'Lilly', 'Lime', 'Limonia (status)', 'Linseed', 'Lint', 'liquor turmeric', 'Litchi', 'Little gourd (Kundru)', 'Long Melon(Kakri)', 'Lotus', 'Lotus Sticks', 'Lukad', 'Lupine', 'Ma.Inji', 'Mace', 'macoy', 'Mahedi', 'Mahua', 'Mahua Seed(Hippe seed)', 'Maida Atta', 'Maize', 'Mango', 'Mango (Raw-Ripe)', 'mango powder', 'Maragensu', 'Marasebu', 'Marget', 'Marigold(Calcutta)', 'Marigold(loose)', 'Marikozhunthu', 'Mash', 'Mashrooms', 'Masur Dal', 'Mataki', 'Methi Seeds', 'Methi(Leaves)', 'Millets', 'Mint(Pudina)', 'Moath Dal', 'Moath Dal', 'Mousambi(Sweet Lime)', 'Muesli', 'Muleti', 'Muskmelon Seeds', 'Mustard', 'Mustard Oil', 'Myrobolan(Harad)', 'Nargasi', 'Nearle Hannu', 'Neem Seed', 'Nelli Kai', 'Nerium', 'nigella seeds', 'nigella seeds', 'Niger Seed (Ramtil)', 'Nutmeg', 'Onion', 'Onion Green', 'Orange', 'Orchid', 'Other green and fresh vegetables', 'Other Pulses', 'Ox', 'Paddy(Dhan)(Basmati)', 'Paddy(Dhan)(Common)', 'Palash flowers', 'Papaya', 'Papaya (Raw)', 'Patti Calcutta', 'Peach', 'Pear(Marasebu)', 'Peas cod', 'Peas Wet', 'Peas(Dry)', 'Pegeon Pea (Arhar Fali)', 'Pepper garbled', 'Pepper ungarbled', 'Perandai', 'Persimon(Japani Fal)', 'Pigs', 'Pineapple', 'pippali', 'Plum', 'Pointed gourd (Parval)', 'Polherb', 'Pomegranate', 'Poppy capsules', 'poppy seeds', 'Potato', 'Pumpkin', 'Pundi', 'Pundi Seed', 'Pupadia', 'Raddish', 'Ragi (Finger Millet)', 'Raibel', 'Rajgir', 'Rala', 'Ram', 'Ramphal', 'Rat Tail Radish (Mogari)', 'Ratanjot', 'Raya', 'Rayee', 'Red Cabbage', 'Red Gram', 'Resinwood', 'Riccbcan', 'Rice', 'Ridgeguard(Tori)', 'Rose(Local)', 'Rose(Loose))', 'Rose(Tata)', 'Round gourd', 'Rubber', 'Sabu Dan', 'Safflower', 'Saffron', 'Sajje', 'salvia', 'Same/Savi', 'sanay', 'Sandalwood', 'Sarasum', 'Season Leaves', 'Seegu', 'Seemebadnekai', 'Seetapal', 'Sesamum(Sesame,Gingelly,Til)', 'sevanti', 'She Buffalo', 'She Goat', 'Sheep', 'Siddota', 'Siru Kizhagu', 'Skin And Hide', 'Snakeguard', 'Soanf', 'Soapnut(Antawala/Retha)', 'Soha', 'Soji', 'Sompu', 'Soyabean', 'spikenard', 'Spinach', 'Sponge gourd', 'Squash(Chappal Kadoo)', 'stevia', 'stone pulverizer', 'Sugar', 'Sugarcane', 'Sundaikai', 'Sunflower', 'Sunflower Seed', 'Sunhemp', 'Suram', 'Surat Beans (Papadi)', 'Suva (Dill Seed)', 'Suvarna Gadde', 'Sweet Potato', 'Sweet Pumpkin', 'Sweet Sultan', 'sweet william', 'T.V. Cumbu', 'Tamarind Fruit', 'Tamarind Seed', 'Tapioca', 'Taramira', 'Tea', 'Tender Coconut', 'Thinai (Italian Millet)', 'Thogrikai', 'Thondekai', 'Tinda', 'Tobacco', 'Tomato', 'Torchwood', 'Toria', 'Tube Flower', 'Tube Rose(Double)', 'Tube Rose(Loose)', 'Tube Rose(Single)', 'Tulasi', 'tulip', 'Turmeric', 'Turmeric (raw)', 'Turnip', 'vadang', 'Vatsanabha', 'Walnut', 'Water Apple', 'Water chestnut', 'Water Melon', 'Wax', 'Wheat', 'Wheat Atta', 'White Muesli', 'White Peas', 'White Pumpkin', 'Wild lemon', 'Wood', 'Wood Apple', 'Wool', 'Yam', 'Yam (Ratalu)']

# commodity_number= [451, 137, 281, 325, 166, 86, 130, 417, 355, 102, 419, 209, 379, 17, 326, 140, 49, 260, 444, 505, 83, 506, 443, 434, 232, 507, 269, 418, 491, 28, 274, 482, 204, 19, 90, 483, 485, 484, 29, 435, 321, 94, 262, 157, 508, 263, 6, 357, 143, 41, 85, 448, 113, 51, 81, 8, 264, 38, 380, 189, 82, 449, 290, 497, 35, 487, 293, 320, 214, 284, 224, 272, 416, 154, 480, 215, 354, 205, 164, 40, 375, 153, 238, 36, 270, 123, 34, 188, 438, 169, 241, 295, 328, 71, 26, 88, 509, 167, 402, 231, 316, 467, 478, 105, 80, 315, 368, 104, 138, 266, 112, 45, 318, 129, 43, 108, 472, 15, 99, 212, 92, 89, 159, 42, 486, 352, 382, 91, 273, 410, 69, 442, 476, 492, 495, 423, 168, 132, 345, 278, 370, 163, 367, 361, 296, 64, 221, 206, 366, 510, 365, 121, 298, 350, 471, 25, 249, 452, 276, 27, 103, 364, 363, 462, 219, 353, 475, 511, 494, 359, 294, 22, 165, 87, 346, 9, 265, 50, 267, 268, 10, 314, 312, 75, 413, 185, 453, 454, 461, 74, 279, 469, 252, 512, 216, 474, 369, 125, 236, 124, 119, 473, 299, 344, 465, 256, 182, 513, 406, 151, 175, 184, 376, 229, 250, 5, 16, 210, 362, 317, 230, 501, 456, 457, 233, 115, 439, 187, 305, 481, 61, 514, 372, 336, 177, 117, 458, 459, 243, 114, 415, 426, 155, 515, 96, 171, 310, 63, 378, 180, 470, 67, 280, 432, 351, 302, 304, 403, 339, 337, 479, 504, 107, 427, 411, 335, 371, 288, 4, 20, 172, 422, 225, 181, 407, 235, 405, 502, 60, 340, 259, 93, 47, 46, 237, 360, 258, 95, 77, 446, 428, 516, 12, 324, 142, 245, 222, 126, 223, 500, 424, 445, 98, 106, 23, 358, 18, 381, 420, 97, 213, 414, 2, 441, 72, 313, 404, 331, 330, 308, 174, 347, 301, 109, 110, 489, 327, 220, 21, 431, 329, 303, 240, 190, 425, 421, 24, 84, 254, 128, 447, 161, 30, 409, 248, 517, 282, 518, 307, 460, 65, 519, 493, 7, 322, 62, 3, 160, 374, 228, 373, 306, 111, 291, 59, 338, 271, 468, 122, 433, 450, 247, 277, 253, 176, 201, 11, 464, 217, 283, 218, 183, 490, 226, 156, 135, 207, 520, 286, 246, 13, 455, 342, 311, 332, 440, 430, 48, 150, 488, 14, 285, 139, 242, 300, 255, 178, 152, 173, 466, 477, 120, 261, 208, 100, 76, 44, 200, 116, 170, 162, 349, 141, 78, 323, 66, 234, 401, 408, 377, 503, 463, 39, 309, 341, 436, 437, 343, 496, 521, 73, 522, 1, 287, 429, 412, 158, 498, 203, 499, 348, 244, 297]
crop_list=['Cotton']
commodity_number= [15]

# rows = []
def cot11():
    for i in range(len(crop_list)):
        today_time=datetime.now().strftime("%d-%B-%Y")
        # print(today_time)
        # url="https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity=15&Tx_State=GJ&Tx_District=0&Tx_Market=0&DateFrom=05-Dec-2024&DateTo=05-Dec-2024&Fr_Date=05-Dec-2024&To_Date=05-Dec-2024&Tx_Trend=2&Tx_CommodityHead=Cotton&Tx_StateHead=Gujarat&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"
        url = f"https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity={commodity_number[i]}&Tx_State=GJ&Tx_District=0&Tx_Market=0&DateFrom={today_time}&DateTo={today_time}&Fr_Date={today_time}&To_Date={today_time}&Tx_Trend=2&Tx_CommodityHead={crop_list[i]}&Tx_StateHead=Gujarat&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        # print(soup)
        table = soup.find('table')
        if table is None:
            print(f"No table found for {crop_list[i]}")
            continue
        
        # print(response.headers)
        # print("table",table)


        rows = []
        for row in table.find_all('tr'):
            print(row)
            current_rows=[]
            for td in row.find_all('td'):
                current_rows.append(td.text.strip())
            print('check : --- ', current_rows)
            print('-'*80)
            if current_rows.count("-")>3:
                continue
            if len(current_rows)>0:
                rt=datetime.strptime(current_rows[-1],"%d %b %Y").strftime("%d-%m-%Y")
                current_rows[-1]=rt
                rows.append(current_rows)
        # rows=rows[:-2]
        headers = [th.text.strip() for th in table.find_all('th')]

        df = pd.DataFrame(rows, columns=headers)
        
        invalid_chars = r'<>:"/\|?*()'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()
    df.to_csv(rf'D:\cott1111{crop_list[i]}_{today_time}.csv', index=False)
    print(f"Data saved for {crop_list[i]}_{today_time}")


cot11()

# schedule.every().day.at("11:40").do(cot11())
# while True:
#     schedule.run_pending()
#     time.sleep(1)