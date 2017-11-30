    # -*- coding: UTF-8 -*-

    # 由于C++编译的时候对函数名做了一些改动，
    # 我们按照改动规则使用诸如：
    #    MY_NLPIR_Init = getattr(dll, '?NLPIR_Init@@YA_NPBDH@Z')
    # 的语句即可找到具体的函数。然后直接使用即可~~~（用ctypes对参数和返回值的类型做一点改动！）
    #
    #
    # 我的具体所有包装代码如下:
    # (需要注意到，只有32-bit的python解释器才能load这个dll库)。


from ctypes import *
import codecs

dll =  CDLL('NLPIR.dll')

def fillprototype(f, restype, argtypes):
    f.restype = restype
    f.argtypes = argtypes


    MY_NLPIR_Init = getattr(dll, '?NLPIR_Init@@YA_NPBDH@Z')
    MY_NLPIR_Exit = getattr(dll, '?NLPIR_Exit@@YA_NXZ')
    MY_NLPIR_ParagraphProcess = getattr(dll, '?NLPIR_ParagraphProcess@@YAPBDPBDH@Z')
    MY_NLPIR_ImportUserDict = getattr(dll, '?NLPIR_ImportUserDict@@YAIPBD@Z')
    MY_NLPIR_FileProcess = getattr(dll, '?NLPIR_FileProcess@@YANPBD0H@Z')
    MY_NLPIR_AddUserWord = getattr(dll, '?NLPIR_AddUserWord@@YAHPBD@Z')
    MY_NLPIR_SaveTheUsrDic = getattr(dll, '?NLPIR_SaveTheUsrDic@@YAHXZ')
    MY_NLPIR_DelUsrWord = getattr(dll, '?NLPIR_DelUsrWord@@YAHPBD@Z')
    MY_NLPIR_GetKeyWords = getattr(dll, '?NLPIR_GetKeyWords@@YAPBDPBDH_N@Z')
    MY_NLPIR_GetFileKeyWords = getattr(dll, '?NLPIR_GetFileKeyWords@@YAPBDPBDH_N@Z')
    MY_NLPIR_GetNewWords = getattr(dll, '?NLPIR_GetNewWords@@YAPBDPBDH_N@Z')
    MY_NLPIR_GetFileNewWords = getattr(dll, '?NLPIR_GetFileNewWords@@YAPBDPBDH_N@Z')
    MY_NLPIR_SetPOSmap = getattr(dll, '?NLPIR_SetPOSmap@@YAHH@Z')
    MY_NLPIR_FingerPrint = getattr(dll, '?NLPIR_FingerPrint@@YAKPBD@Z')
    #New Word Identification
    MY_NLPIR_NWI_Start = getattr(dll, '?NLPIR_NWI_Start@@YA_NXZ')
    MY_NLPIR_NWI_AddFile = getattr(dll, '?NLPIR_NWI_AddFile@@YAHPBD@Z')
    MY_NLPIR_NWI_AddMem = getattr(dll, '?NLPIR_NWI_AddMem@@YA_NPBD@Z')
    MY_NLPIR_NWI_Complete = getattr(dll, '?NLPIR_NWI_Complete@@YA_NXZ')
    MY_NLPIR_NWI_GetResult = getattr(dll, '?NLPIR_NWI_GetResult@@YAPBD_N@Z')
    MY_NLPIR_NWI_Result2UserDict = getattr(dll, '?NLPIR_NWI_Result2UserDict@@YAIXZ')

    fillprototype(MY_NLPIR_Init, c_bool, [c_char_p, c_int])
    fillprototype(MY_NLPIR_Exit, c_bool, None)
    fillprototype(MY_NLPIR_ParagraphProcess, c_char_p, [c_char_p, c_int])
    fillprototype(MY_NLPIR_ImportUserDict, c_uint, [c_char_p])
    fillprototype(MY_NLPIR_FileProcess, c_double, [c_char_p, c_char_p, c_int])
    fillprototype(MY_NLPIR_AddUserWord, c_int, [c_char_p])
    fillprototype(MY_NLPIR_SaveTheUsrDic, c_int, None)
    fillprototype(MY_NLPIR_DelUsrWord, c_int, [c_char_p])
    fillprototype(MY_NLPIR_GetKeyWords, c_char_p, [c_char_p, c_int, c_bool])
    fillprototype(MY_NLPIR_GetFileKeyWords, c_char_p, [c_char_p, c_int, c_bool])
    fillprototype(MY_NLPIR_GetNewWords, c_char_p, [c_char_p, c_int, c_bool])
    fillprototype(MY_NLPIR_GetFileNewWords, c_char_p, [c_char_p, c_int, c_bool])
    fillprototype(MY_NLPIR_SetPOSmap, c_int, [c_int])
    fillprototype(MY_NLPIR_FingerPrint, c_ulong, [c_char_p])
    #New Word Identification
    fillprototype(MY_NLPIR_NWI_Start, c_bool, None)
    fillprototype(MY_NLPIR_NWI_AddFile, c_bool, [c_char_p])
    fillprototype(MY_NLPIR_NWI_AddMem, c_bool, [c_char_p])
    fillprototype(MY_NLPIR_NWI_Complete, c_bool, None)
    fillprototype(MY_NLPIR_NWI_GetResult, c_char_p, [c_int])
    fillprototype(MY_NLPIR_NWI_Result2UserDict, c_uint, None)


    look_gb = codecs.lookup('gb2312')
    look_utf = codecs.lookup('utf-8')

    if not MY_NLPIR_Init('',1):
        print "Initial fail"
        exit()

    sentence = u"我爱我的祖国亲爱的祖国！"


    # a3 = look_utf.decode(s2)
    # print a2
    # result="我爱我的祖国亲爱的祖国！"
    result = MY_NLPIR_ParagraphProcess(sentence, c_int(1))


    result_unicode = look_utf.decode(result)[0]
    result_gb2312 = look_gb.encode(result_unicode)[0]
    result_gbk = look_gb.decode(result_gb2312)[0]

    p = codecs.open('./aaaaaaaaaaaaa','w','utf-8')
    # p = codecs.open('./aaaaaaaaaaaaa','w','gb2312')
    p.write(result_unicode)
    p.write(result_gbk)
    p.close()
    for i in open('./aaaaaaaaaaaaa'):
        print i


    MY_NLPIR_Exit()

    print "The End"

fillprototype
