--bacth 3
set serveroutput on size 1000000;
declare
   cursor c_global_id
    is
        select global_id
        from end_frg_content_data
        where global_id in (9200000007087766,9200000021914290,9200000026727350,1001024253158719,1002004011879331,9200000011396340,9200000021739735,1002004005471634,1001024211237001,1002004013406805,1001024268713165,1001024252215368,1001024259346675,1001024214424263,1001024257585349,1001024040597444,1001024243823620,1001024221372582,9200000021022568,1002004006043234,1002004006067640,9200000012831811,1001024245788119,1001024275595214,1001024183316332,1000004007503775,1000004000002260,1000004011656408,1000004011661735,1000004006045128,9200000024523626,1002004010716707,1002004012016520,1002004011077154,1001024211357977,1001024194444204,1001024241321218,9200000012823728,1000004004679534,1002004012184462,9200000018346772,1001004001241560,9200000010719717,1000004013378334,9200000019246299,9200000002061806,9200000005556786,1002004000084760,1000004013674546,1001024170323082,1002004013093612,1002004006740839,1001024247816052,9200000010403700,9200000026644738,9200000018982379,9200000011436823,1001004001241561,9200000001236492,1000004013107650,1000004013107457,9200000020712799,1001004008974939,9200000010046942,1001024267696537,1001004007470392,1001024218317295,1001024264565562,1001024159846530,9200000024512052,1001024268081878,1001024074943831,1002004008667818,9200000025449809,9200000021822427,1000004012129656,9200000024664030,1002004012261163,1002004013707753,9200000020907483,9200000022074391,9200000021979540,1001024270705512,9200000020389173,1001004005936191,9200000010945963,1001024255399049,9200000024927933,1001004007514574,9200000024948493,9200000005930755,1002004007442661,1002004013627373,9200000015483344,9200000013711504,9200000018976179,1000004007164236,9200000020448214,1000004003142514,9200000022857250,1000004000033685,9000000011874410,1001024240156752,1004004013408309,1001024254809409,1002004011646822,1002004011723835,9200000027199140,1002004013436642,1002004013469821,9200000018321496,9200000017120810,9200000002156607,9200000007432182,9005000012455734,9200000011254769,1001004001241561,9200000022704675,9200000026371730,1001024272096049,1001024257671908,1001004010930266,9200000005531216,9200000001279424,9200000001279556,9200000024753963,9200000017940267,9200000020907530,1001024265820785,9200000026878662,9200000002310757,1000004003052384,1001024271888565,9200000024674125,1002004000044906,9200000024749059,9200000010998010,1001024274015579,1001024146989481,1001024096372956,1001004002015353,1001004001241561,1001004005470003,1002004000037883,1001024096372999,9200000011356347,9200000005539794,9200000011511268,9200000002156607,9200000007432182,9005000012455734,9200000011254769,1001004001241561,9200000009423326,9200000014054919,9200000005033067,9200000014054899,1001024270570953,1001024222060065,1001024247783615,9200000003349360,9200000003349364,1000004013447178,9200000019517398,1000004003097245,1001024244005689,1001024226247665,9200000024780349,9000000011949700,9200000002307368,9200000002307364,1004004010948491,1001024263391940,9200000014613511,9200000000666823,1002004013217852,9200000007042257,1001004001573148,9200000009935198,9200000014371595,9200000020504466,1001024272543484,9200000014054899,1001004005649600,9200000014613489,9200000014613495,9200000022070973,9200000026765222,1001024272564746,1001024252203313,1000004001205179,1000004013359734,1000004001125824,1000004005737684,1001024127847293,9200000005930755,9200000022121785,9200000026257152,1001004007630763,9200000007510923,9200000014373827,9200000005138911,9200000005033618,9200000010559155,9200000010559159,9200000009541297,9200000009380506,1001004001241561,9200000022121767,9200000021705906,9200000026160281,1001004011831328,1004004013461314,1004004012214821,9200000010943812,1001024210617034,9200000022121767,9200000021741183,9200000025422240,1001004006271819,9200000026791370,9200000022071147,9000000012399690,1001004006885968,1001004001003491,9200000027104408,1001024242675382,9000000011035689,9200000010673583,9200000011363682,9200000026257198,1001004009993132,1004004013556913,1001024272072468,9200000002756034,9200000026769561,9005000012120597,9200000010112856,9200000014964274,9200000000663803,9200000007041993,1000004011634736,9200000024537289,1000004013024316,9200000025427195,9200000021155871,1001024171646075,1001024096372878,9200000006547020,9200000024640232,1001024083311345,1002004006067640,1002004006043234,9200000019405791,1001024270803842,9200000021230272,9200000015044802,1002004005043746,1002004012261219,1001024275971832,1004004013309941,1001024274254535,1001024276148478,9200000005138905,9200000022121523,1001024276347328,9200000021705906,9200000005483946,9200000026769541,9200000002920649,9200000002920667,1001004000924302,9200000011256133,1001004011731947,1002004010935280,1002004013459587,9200000025828328,1002004005043746,1002004011694057,1002004013018034,1002004013134337,9200000010394210,9200000026744330,1001024189122887,9200000005185579,1001024020704682,1001004011371698,1001024116968537,1001024267345383,1004004013378818,1001024199762317,1001024241545354,1004004011684818,1001024193190547,9200000021924414,9200000009370360,1001004002948522,9200000019401207,1001024270377911,9200000005185543,9200000005191428,9200000009423288,9200000012046554,1001004001241560,1000004010291147,1001024276423011,9200000005556114,1001004001241561,1004004011231731,1001024238091936,1001024160786582,1001024260021573,1001024145309294,1001024231629713,9200000026077255,9200000022666711,9200000025817162,9200000002308303,9200000010834164,9200000007032377,1001004001241561,1001004006456031,1001004006149304,9200000026739211,9200000024641797,9200000024537289,1000004013459657,1000004013024316,1001024258540473,9200000009585146,9200000026169207,1000004011754229,9200000026196003,1000004003087233,1002004013146440,9200000006236976,1002004000002238,1002004000002230,9200000010962221,9200000020049270,1002004012281315,1002004009068866,9200000010039338,1002004012415793,9200000022456671,9200000015334104,9200000015334132,9200000002405490,1001004002265949,1001004010926490,1000004007526367,1002004006275483,9200000026787899,9200000025828328,9200000025883552,1002004005043746,1001004009694533,1002004013188341,9200000010693928,9200000002208213,1001004007684001,1001004000746118,1001024190958302,1001004007638123,1001004007683984,1001004010951317,9200000025107835,9200000002753497,1002004013418568,9200000022666675,9200000025468722,9200000023670575,9200000025427195,1000004005425623,1000004001301184,1000004006301396,1000004012558972,1000004010675180,1000004011187928,1000004012016524,9200000005180075,9200000005180111,9200000005180113,1002004011315510,1002004012256555,1002004012518617,1002004013594170,9200000015012071,9200000005228382,1001004001241561,1001024278693972,9200000026169207,9200000021819369,9200000025427087,1001024279764983,9200000009652103,1001024265157842,9200000018727599,1001024279764983,1001024277447850,1001024264285415,9200000011366767,1001004001241560,9200000011366767,1001004001241560,9200000026169207,9200000005156785,1001024088134742,1002004004390007,1002004008138993,1002004000076077,9200000025427195,9200000005179913,9200000011356731,9200000022092064,9200000022070901,9200000010081320,1000004001125824,1000004005737684,1000004001192054,9200000026359055,9200000024777769,1002004011512662,1002004013605740,9200000018734557,1002004013483967,1001004006216357,9000000011035751,9200000026504417,9200000015015960,9200000005250764,9200000026169207,1003004012146677,1002004004781120,1002004011028104,1002004011568035,9200000020686501,1002004006434373,9200000022666107,1002004011742504,9200000015502514,1001004006551594,1001004002545683,1001004005513191,1004004007689877,9200000007042092,1002004013601155,9200000018727595,9200000015496508,9200000026769561,1001024259298478,9200000009984496,1004004007111725,9200000010998008,9200000010998016,9200000018981255,9000000011224828,1002004000122171,1002004000093318,1004004013014197,9200000013360843,9200000027955936,1000004006237184,9200000024784246,9200000010036149,9200000010395848,9200000008205533,9200000005180855,1002004011966822,1002004012159912,1002004011077149,1001024273175935,1001024201712364,1001024266578055,1000004000046498,1000004013612218,1001024277401087,1002004006762458,9200000010762788,1003004012364375,1001024279145566,1001004001490132,1001004006485638,9200000015594736,9200000014403802,9200000020425860,9200000010755849,1002004000119393,1000004001797885,9200000020658436,1000004006554300,1001024082818422,1000004006244431,9200000025427195,9200000027398484,1004004011501823,1004004013009884,1004004012542276,9200000022070911,1000004011396536,1000004011754587,9200000026169207,9000000012246566,1001024058462385,1001024186593617,1000004011092757,1000004006574334,1000004004486177,1000004006406588,1004004011781755,9200000011255063,9200000009942167,1001004001241561,1002004000001433,1001024276285362,9200000005483946,1000004003217690,1000004005962573,9200000022068634,1002004013374541,1002004012341856,9200000018217811,9200000018976179,1002004010975339,1002004013594169,1002004012340162,9200000019690644,9000000012862196,9200000020075035,9200000011675472,1004004012385725,9200000009850272,9200000026920007,1002004006478909,9200000014958628,9200000023778512,9200000002544881,9200000026169207,1001004011156758,9200000026169216,9200000013383978,9200000024532500,9200000014371288,1001024077772413,9200000010697891,9200000011167609,1004004012558190,9200000027104033,9200000025077882,9200000022701224,1000004011704088,1000004003204695,9200000026501006,9200000026454912,9200000020715527,1000004013520174,9200000025828328,9200000020318581,1001024277695520,1001024270080223,1001024122211103,9200000019888905,9200000008860713,1001004000641776,9200000002526920,1001024280493093,9200000021001712,9200000015478591,9200000017940269,9200000021231488,1004004012158633,1000004000034192,1001024210590198,1002004006208108,1001024279927769,1001024280688021,1002004013564812,1002004008663166,1002004000078351,1002004011450450,1002004005449421,9200000014684131,9200000010412600,666865696,9200000015464332,9200000015464280,1000004003019160,1002004006279263,1001004011754423,1001004007082251,9200000015001710,9200000024753937,9200000007510923,9200000014373827,1001024082161627,9200000005227368,9200000005033814,9200000005180003,9200000005178939,9200000022446621,1002004005744058,1001024025263884,1001024069459349,1001024227110175,1000004012368977,9200000021226933,1001004009696309,9200000022637037,9200000011452700,1001024283281482,1001024283670577,9200000028101559,9200000014652651,9005000012201542,1001024281128803,9200000015051999,9200000020471239,1002004013208362,1002004012018754,9200000020952272,9200000027388514,1004004012367275,9200000021231488,1002004012010767,9200000018197672,1002004011678480,9200000027113202,1001024282029395,1001024278716821,1001004007251795,1002004006191281,1001024183342390,9200000028127355,9200000018322413,1001024210380759,1002004006560124,1002004006855596￿￿￿ઐ);
                
   type g_idtype is table of c_global_id%rowtype;
   
   g_id g_idtype;
   
   TYPE result IS RECORD
   (
      id                  varchar2(10),
      global_id           VARCHAR2(16),
      category_id         VARCHAR2(10),
      category_desc       VARCHAR2(100)
   );

   cat            varchar2(10);
   catd            varchar2(100);
   subtype num is varchar2(10);
   type tab_type is table of result index by num;
   tab tab_type;
   i     number(10);
   t_idx pls_integer;
    v_filehandle utl_file.file_type;
    
   
begin
   open  c_global_id;
   fetch c_global_id bulk collect
   into  g_id;
   close c_global_id;
    
    i := 1;
    t_idx := g_id.first; 
    
    --v_filehandle := utl_file.fopen('U:\','product-category.txt','a');--Opening a file
          
    while g_id.exists(t_idx)
    LOOP
            tab(i).id := to_char(i);
            tab(i).global_id := g_id(t_idx).global_id;
            
            select REGEXP_SUBSTR(revPath, '[^,]+', 1, 1) into cat
            from
            (
            with relation(parent_id,id) as
            (
            select parent_id, id
            from end_shelf_definitions
            )
            SELECT ListAgg(id,',')
                   within group(order by Level desc) as revPath
            FROM relation
            START WITH id = (select shelf_id from end_shelf_products where global_id = g_id(t_idx).global_id and rownum = 1)
            CONNECT BY PRIOR parent_id = id
            and id <> 1
            );
            
            select nvl(description,'')  into catd
            from end_shelf_definitions where id = nvl(cat,1);
            
            
            tab(i).category_id := cat;
            
            tab(i).category_desc := catd;
            
            dbms_output.put_line(g_id(t_idx).global_id || ',' || catd); 
            
            i := i + 1;
            t_idx := g_id.next(t_idx);
    
            --utl_file.putf(v_filehandle,'%s, %s\n',g_id(t_idx).global_id, catd);                    
            --utl_file.new_line(v_filehandle);
    END LOOP;
    
    --UTL_FILE.fclose(v_filehandle);

end;        


