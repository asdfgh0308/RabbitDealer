#ifndef __zlog_h
#define __zlog_h

#define dzlog_fatal(...) ;
#define dzlog_error(...) printf("%s:%d %s , ERROR happens here!\n", __FILE__,__LINE__,__func__);
#define dzlog_warn(...) ;
#define dzlog_notice(...) ;
#define dzlog_info(...) ;
#define dzlog_debug(...) ;


#endif
