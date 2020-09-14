create or replace function u2.checksum_from_insert_rows(arr integer[])
   returns integer
   language plpgsql
as $function$
declare
  checksum  int not null := 0;
  val       int not null := 0;
  j         int not null := 0;
  new_k     int not null := 0;
  new_val   int not null := 0;
begin
  foreach val in array arr loop
    case j
      when 000 then
        insert into t000(v) values(val) returning k into new_k;
        select v into new_val from t000 where k = new_k;
        checksum = checksum + new_val;
      when 001 then
        insert into t001(v) values(val) returning k into new_k;
        select v into new_val from t001 where k = new_k;
        checksum = checksum + new_val;
      when 002 then
        insert into t002(v) values(val) returning k into new_k;
        select v into new_val from t002 where k = new_k;
        checksum = checksum + new_val;
      when 003 then
        insert into t003(v) values(val) returning k into new_k;
        select v into new_val from t003 where k = new_k;
        checksum = checksum + new_val;
      when 004 then
        insert into t004(v) values(val) returning k into new_k;
        select v into new_val from t004 where k = new_k;
        checksum = checksum + new_val;
      when 005 then
        insert into t005(v) values(val) returning k into new_k;
        select v into new_val from t005 where k = new_k;
        checksum = checksum + new_val;
      when 006 then
        insert into t006(v) values(val) returning k into new_k;
        select v into new_val from t006 where k = new_k;
        checksum = checksum + new_val;
      when 007 then
        insert into t007(v) values(val) returning k into new_k;
        select v into new_val from t007 where k = new_k;
        checksum = checksum + new_val;
      when 008 then
        insert into t008(v) values(val) returning k into new_k;
        select v into new_val from t008 where k = new_k;
        checksum = checksum + new_val;
      when 009 then
        insert into t009(v) values(val) returning k into new_k;
        select v into new_val from t009 where k = new_k;
        checksum = checksum + new_val;
      when 010 then
        insert into t010(v) values(val) returning k into new_k;
        select v into new_val from t010 where k = new_k;
        checksum = checksum + new_val;
      when 011 then
        insert into t011(v) values(val) returning k into new_k;
        select v into new_val from t011 where k = new_k;
        checksum = checksum + new_val;
      when 012 then
        insert into t012(v) values(val) returning k into new_k;
        select v into new_val from t012 where k = new_k;
        checksum = checksum + new_val;
      when 013 then
        insert into t013(v) values(val) returning k into new_k;
        select v into new_val from t013 where k = new_k;
        checksum = checksum + new_val;
      when 014 then
        insert into t014(v) values(val) returning k into new_k;
        select v into new_val from t014 where k = new_k;
        checksum = checksum + new_val;
      when 015 then
        insert into t015(v) values(val) returning k into new_k;
        select v into new_val from t015 where k = new_k;
        checksum = checksum + new_val;
      when 016 then
        insert into t016(v) values(val) returning k into new_k;
        select v into new_val from t016 where k = new_k;
        checksum = checksum + new_val;
      when 017 then
        insert into t017(v) values(val) returning k into new_k;
        select v into new_val from t017 where k = new_k;
        checksum = checksum + new_val;
      when 018 then
        insert into t018(v) values(val) returning k into new_k;
        select v into new_val from t018 where k = new_k;
        checksum = checksum + new_val;
      when 019 then
        insert into t019(v) values(val) returning k into new_k;
        select v into new_val from t019 where k = new_k;
        checksum = checksum + new_val;
      when 020 then
        insert into t020(v) values(val) returning k into new_k;
        select v into new_val from t020 where k = new_k;
        checksum = checksum + new_val;
      when 021 then
        insert into t021(v) values(val) returning k into new_k;
        select v into new_val from t021 where k = new_k;
        checksum = checksum + new_val;
      when 022 then
        insert into t022(v) values(val) returning k into new_k;
        select v into new_val from t022 where k = new_k;
        checksum = checksum + new_val;
      when 023 then
        insert into t023(v) values(val) returning k into new_k;
        select v into new_val from t023 where k = new_k;
        checksum = checksum + new_val;
      when 024 then
        insert into t024(v) values(val) returning k into new_k;
        select v into new_val from t024 where k = new_k;
        checksum = checksum + new_val;
      when 025 then
        insert into t025(v) values(val) returning k into new_k;
        select v into new_val from t025 where k = new_k;
        checksum = checksum + new_val;
      when 026 then
        insert into t026(v) values(val) returning k into new_k;
        select v into new_val from t026 where k = new_k;
        checksum = checksum + new_val;
      when 027 then
        insert into t027(v) values(val) returning k into new_k;
        select v into new_val from t027 where k = new_k;
        checksum = checksum + new_val;
      when 028 then
        insert into t028(v) values(val) returning k into new_k;
        select v into new_val from t028 where k = new_k;
        checksum = checksum + new_val;
      when 029 then
        insert into t029(v) values(val) returning k into new_k;
        select v into new_val from t029 where k = new_k;
        checksum = checksum + new_val;
      when 030 then
        insert into t030(v) values(val) returning k into new_k;
        select v into new_val from t030 where k = new_k;
        checksum = checksum + new_val;
      when 031 then
        insert into t031(v) values(val) returning k into new_k;
        select v into new_val from t031 where k = new_k;
        checksum = checksum + new_val;
      when 032 then
        insert into t032(v) values(val) returning k into new_k;
        select v into new_val from t032 where k = new_k;
        checksum = checksum + new_val;
      when 033 then
        insert into t033(v) values(val) returning k into new_k;
        select v into new_val from t033 where k = new_k;
        checksum = checksum + new_val;
      when 034 then
        insert into t034(v) values(val) returning k into new_k;
        select v into new_val from t034 where k = new_k;
        checksum = checksum + new_val;
      when 035 then
        insert into t035(v) values(val) returning k into new_k;
        select v into new_val from t035 where k = new_k;
        checksum = checksum + new_val;
      when 036 then
        insert into t036(v) values(val) returning k into new_k;
        select v into new_val from t036 where k = new_k;
        checksum = checksum + new_val;
      when 037 then
        insert into t037(v) values(val) returning k into new_k;
        select v into new_val from t037 where k = new_k;
        checksum = checksum + new_val;
      when 038 then
        insert into t038(v) values(val) returning k into new_k;
        select v into new_val from t038 where k = new_k;
        checksum = checksum + new_val;
      when 039 then
        insert into t039(v) values(val) returning k into new_k;
        select v into new_val from t039 where k = new_k;
        checksum = checksum + new_val;
      when 040 then
        insert into t040(v) values(val) returning k into new_k;
        select v into new_val from t040 where k = new_k;
        checksum = checksum + new_val;
      when 041 then
        insert into t041(v) values(val) returning k into new_k;
        select v into new_val from t041 where k = new_k;
        checksum = checksum + new_val;
      when 042 then
        insert into t042(v) values(val) returning k into new_k;
        select v into new_val from t042 where k = new_k;
        checksum = checksum + new_val;
      when 043 then
        insert into t043(v) values(val) returning k into new_k;
        select v into new_val from t043 where k = new_k;
        checksum = checksum + new_val;
      when 044 then
        insert into t044(v) values(val) returning k into new_k;
        select v into new_val from t044 where k = new_k;
        checksum = checksum + new_val;
      when 045 then
        insert into t045(v) values(val) returning k into new_k;
        select v into new_val from t045 where k = new_k;
        checksum = checksum + new_val;
      when 046 then
        insert into t046(v) values(val) returning k into new_k;
        select v into new_val from t046 where k = new_k;
        checksum = checksum + new_val;
      when 047 then
        insert into t047(v) values(val) returning k into new_k;
        select v into new_val from t047 where k = new_k;
        checksum = checksum + new_val;
      when 048 then
        insert into t048(v) values(val) returning k into new_k;
        select v into new_val from t048 where k = new_k;
        checksum = checksum + new_val;
      when 049 then
        insert into t049(v) values(val) returning k into new_k;
        select v into new_val from t049 where k = new_k;
        checksum = checksum + new_val;
      when 050 then
        insert into t050(v) values(val) returning k into new_k;
        select v into new_val from t050 where k = new_k;
        checksum = checksum + new_val;
      when 051 then
        insert into t051(v) values(val) returning k into new_k;
        select v into new_val from t051 where k = new_k;
        checksum = checksum + new_val;
      when 052 then
        insert into t052(v) values(val) returning k into new_k;
        select v into new_val from t052 where k = new_k;
        checksum = checksum + new_val;
      when 053 then
        insert into t053(v) values(val) returning k into new_k;
        select v into new_val from t053 where k = new_k;
        checksum = checksum + new_val;
      when 054 then
        insert into t054(v) values(val) returning k into new_k;
        select v into new_val from t054 where k = new_k;
        checksum = checksum + new_val;
      when 055 then
        insert into t055(v) values(val) returning k into new_k;
        select v into new_val from t055 where k = new_k;
        checksum = checksum + new_val;
      when 056 then
        insert into t056(v) values(val) returning k into new_k;
        select v into new_val from t056 where k = new_k;
        checksum = checksum + new_val;
      when 057 then
        insert into t057(v) values(val) returning k into new_k;
        select v into new_val from t057 where k = new_k;
        checksum = checksum + new_val;
      when 058 then
        insert into t058(v) values(val) returning k into new_k;
        select v into new_val from t058 where k = new_k;
        checksum = checksum + new_val;
      when 059 then
        insert into t059(v) values(val) returning k into new_k;
        select v into new_val from t059 where k = new_k;
        checksum = checksum + new_val;
      when 060 then
        insert into t060(v) values(val) returning k into new_k;
        select v into new_val from t060 where k = new_k;
        checksum = checksum + new_val;
      when 061 then
        insert into t061(v) values(val) returning k into new_k;
        select v into new_val from t061 where k = new_k;
        checksum = checksum + new_val;
      when 062 then
        insert into t062(v) values(val) returning k into new_k;
        select v into new_val from t062 where k = new_k;
        checksum = checksum + new_val;
      when 063 then
        insert into t063(v) values(val) returning k into new_k;
        select v into new_val from t063 where k = new_k;
        checksum = checksum + new_val;
      when 064 then
        insert into t064(v) values(val) returning k into new_k;
        select v into new_val from t064 where k = new_k;
        checksum = checksum + new_val;
      when 065 then
        insert into t065(v) values(val) returning k into new_k;
        select v into new_val from t065 where k = new_k;
        checksum = checksum + new_val;
      when 066 then
        insert into t066(v) values(val) returning k into new_k;
        select v into new_val from t066 where k = new_k;
        checksum = checksum + new_val;
      when 067 then
        insert into t067(v) values(val) returning k into new_k;
        select v into new_val from t067 where k = new_k;
        checksum = checksum + new_val;
      when 068 then
        insert into t068(v) values(val) returning k into new_k;
        select v into new_val from t068 where k = new_k;
        checksum = checksum + new_val;
      when 069 then
        insert into t069(v) values(val) returning k into new_k;
        select v into new_val from t069 where k = new_k;
        checksum = checksum + new_val;
      when 070 then
        insert into t070(v) values(val) returning k into new_k;
        select v into new_val from t070 where k = new_k;
        checksum = checksum + new_val;
      when 071 then
        insert into t071(v) values(val) returning k into new_k;
        select v into new_val from t071 where k = new_k;
        checksum = checksum + new_val;
      when 072 then
        insert into t072(v) values(val) returning k into new_k;
        select v into new_val from t072 where k = new_k;
        checksum = checksum + new_val;
      when 073 then
        insert into t073(v) values(val) returning k into new_k;
        select v into new_val from t073 where k = new_k;
        checksum = checksum + new_val;
      when 074 then
        insert into t074(v) values(val) returning k into new_k;
        select v into new_val from t074 where k = new_k;
        checksum = checksum + new_val;
      when 075 then
        insert into t075(v) values(val) returning k into new_k;
        select v into new_val from t075 where k = new_k;
        checksum = checksum + new_val;
      when 076 then
        insert into t076(v) values(val) returning k into new_k;
        select v into new_val from t076 where k = new_k;
        checksum = checksum + new_val;
      when 077 then
        insert into t077(v) values(val) returning k into new_k;
        select v into new_val from t077 where k = new_k;
        checksum = checksum + new_val;
      when 078 then
        insert into t078(v) values(val) returning k into new_k;
        select v into new_val from t078 where k = new_k;
        checksum = checksum + new_val;
      when 079 then
        insert into t079(v) values(val) returning k into new_k;
        select v into new_val from t079 where k = new_k;
        checksum = checksum + new_val;
      when 080 then
        insert into t080(v) values(val) returning k into new_k;
        select v into new_val from t080 where k = new_k;
        checksum = checksum + new_val;
      when 081 then
        insert into t081(v) values(val) returning k into new_k;
        select v into new_val from t081 where k = new_k;
        checksum = checksum + new_val;
      when 082 then
        insert into t082(v) values(val) returning k into new_k;
        select v into new_val from t082 where k = new_k;
        checksum = checksum + new_val;
      when 083 then
        insert into t083(v) values(val) returning k into new_k;
        select v into new_val from t083 where k = new_k;
        checksum = checksum + new_val;
      when 084 then
        insert into t084(v) values(val) returning k into new_k;
        select v into new_val from t084 where k = new_k;
        checksum = checksum + new_val;
      when 085 then
        insert into t085(v) values(val) returning k into new_k;
        select v into new_val from t085 where k = new_k;
        checksum = checksum + new_val;
      when 086 then
        insert into t086(v) values(val) returning k into new_k;
        select v into new_val from t086 where k = new_k;
        checksum = checksum + new_val;
      when 087 then
        insert into t087(v) values(val) returning k into new_k;
        select v into new_val from t087 where k = new_k;
        checksum = checksum + new_val;
      when 088 then
        insert into t088(v) values(val) returning k into new_k;
        select v into new_val from t088 where k = new_k;
        checksum = checksum + new_val;
      when 089 then
        insert into t089(v) values(val) returning k into new_k;
        select v into new_val from t089 where k = new_k;
        checksum = checksum + new_val;
      when 090 then
        insert into t090(v) values(val) returning k into new_k;
        select v into new_val from t090 where k = new_k;
        checksum = checksum + new_val;
      when 091 then
        insert into t091(v) values(val) returning k into new_k;
        select v into new_val from t091 where k = new_k;
        checksum = checksum + new_val;
      when 092 then
        insert into t092(v) values(val) returning k into new_k;
        select v into new_val from t092 where k = new_k;
        checksum = checksum + new_val;
      when 093 then
        insert into t093(v) values(val) returning k into new_k;
        select v into new_val from t093 where k = new_k;
        checksum = checksum + new_val;
      when 094 then
        insert into t094(v) values(val) returning k into new_k;
        select v into new_val from t094 where k = new_k;
        checksum = checksum + new_val;
      when 095 then
        insert into t095(v) values(val) returning k into new_k;
        select v into new_val from t095 where k = new_k;
        checksum = checksum + new_val;
      when 096 then
        insert into t096(v) values(val) returning k into new_k;
        select v into new_val from t096 where k = new_k;
        checksum = checksum + new_val;
      when 097 then
        insert into t097(v) values(val) returning k into new_k;
        select v into new_val from t097 where k = new_k;
        checksum = checksum + new_val;
      when 098 then
        insert into t098(v) values(val) returning k into new_k;
        select v into new_val from t098 where k = new_k;
        checksum = checksum + new_val;
      when 099 then
        insert into t099(v) values(val) returning k into new_k;
        select v into new_val from t099 where k = new_k;
        checksum = checksum + new_val;
      when 100 then
        insert into t100(v) values(val) returning k into new_k;
        select v into new_val from t100 where k = new_k;
        checksum = checksum + new_val;
      when 101 then
        insert into t101(v) values(val) returning k into new_k;
        select v into new_val from t101 where k = new_k;
        checksum = checksum + new_val;
      when 102 then
        insert into t102(v) values(val) returning k into new_k;
        select v into new_val from t102 where k = new_k;
        checksum = checksum + new_val;
      when 103 then
        insert into t103(v) values(val) returning k into new_k;
        select v into new_val from t103 where k = new_k;
        checksum = checksum + new_val;
      when 104 then
        insert into t104(v) values(val) returning k into new_k;
        select v into new_val from t104 where k = new_k;
        checksum = checksum + new_val;
      when 105 then
        insert into t105(v) values(val) returning k into new_k;
        select v into new_val from t105 where k = new_k;
        checksum = checksum + new_val;
      when 106 then
        insert into t106(v) values(val) returning k into new_k;
        select v into new_val from t106 where k = new_k;
        checksum = checksum + new_val;
      when 107 then
        insert into t107(v) values(val) returning k into new_k;
        select v into new_val from t107 where k = new_k;
        checksum = checksum + new_val;
      when 108 then
        insert into t108(v) values(val) returning k into new_k;
        select v into new_val from t108 where k = new_k;
        checksum = checksum + new_val;
      when 109 then
        insert into t109(v) values(val) returning k into new_k;
        select v into new_val from t109 where k = new_k;
        checksum = checksum + new_val;
      when 110 then
        insert into t110(v) values(val) returning k into new_k;
        select v into new_val from t110 where k = new_k;
        checksum = checksum + new_val;
      when 111 then
        insert into t111(v) values(val) returning k into new_k;
        select v into new_val from t111 where k = new_k;
        checksum = checksum + new_val;
      when 112 then
        insert into t112(v) values(val) returning k into new_k;
        select v into new_val from t112 where k = new_k;
        checksum = checksum + new_val;
      when 113 then
        insert into t113(v) values(val) returning k into new_k;
        select v into new_val from t113 where k = new_k;
        checksum = checksum + new_val;
      when 114 then
        insert into t114(v) values(val) returning k into new_k;
        select v into new_val from t114 where k = new_k;
        checksum = checksum + new_val;
      when 115 then
        insert into t115(v) values(val) returning k into new_k;
        select v into new_val from t115 where k = new_k;
        checksum = checksum + new_val;
      when 116 then
        insert into t116(v) values(val) returning k into new_k;
        select v into new_val from t116 where k = new_k;
        checksum = checksum + new_val;
      when 117 then
        insert into t117(v) values(val) returning k into new_k;
        select v into new_val from t117 where k = new_k;
        checksum = checksum + new_val;
      when 118 then
        insert into t118(v) values(val) returning k into new_k;
        select v into new_val from t118 where k = new_k;
        checksum = checksum + new_val;
      when 119 then
        insert into t119(v) values(val) returning k into new_k;
        select v into new_val from t119 where k = new_k;
        checksum = checksum + new_val;
      when 120 then
        insert into t120(v) values(val) returning k into new_k;
        select v into new_val from t120 where k = new_k;
        checksum = checksum + new_val;
      when 121 then
        insert into t121(v) values(val) returning k into new_k;
        select v into new_val from t121 where k = new_k;
        checksum = checksum + new_val;
      when 122 then
        insert into t122(v) values(val) returning k into new_k;
        select v into new_val from t122 where k = new_k;
        checksum = checksum + new_val;
      when 123 then
        insert into t123(v) values(val) returning k into new_k;
        select v into new_val from t123 where k = new_k;
        checksum = checksum + new_val;
      when 124 then
        insert into t124(v) values(val) returning k into new_k;
        select v into new_val from t124 where k = new_k;
        checksum = checksum + new_val;
      when 125 then
        insert into t125(v) values(val) returning k into new_k;
        select v into new_val from t125 where k = new_k;
        checksum = checksum + new_val;
      when 126 then
        insert into t126(v) values(val) returning k into new_k;
        select v into new_val from t126 where k = new_k;
        checksum = checksum + new_val;
      when 127 then
        insert into t127(v) values(val) returning k into new_k;
        select v into new_val from t127 where k = new_k;
        checksum = checksum + new_val;
      when 128 then
        insert into t128(v) values(val) returning k into new_k;
        select v into new_val from t128 where k = new_k;
        checksum = checksum + new_val;
      when 129 then
        insert into t129(v) values(val) returning k into new_k;
        select v into new_val from t129 where k = new_k;
        checksum = checksum + new_val;
      when 130 then
        insert into t130(v) values(val) returning k into new_k;
        select v into new_val from t130 where k = new_k;
        checksum = checksum + new_val;
      when 131 then
        insert into t131(v) values(val) returning k into new_k;
        select v into new_val from t131 where k = new_k;
        checksum = checksum + new_val;
      when 132 then
        insert into t132(v) values(val) returning k into new_k;
        select v into new_val from t132 where k = new_k;
        checksum = checksum + new_val;
      when 133 then
        insert into t133(v) values(val) returning k into new_k;
        select v into new_val from t133 where k = new_k;
        checksum = checksum + new_val;
      when 134 then
        insert into t134(v) values(val) returning k into new_k;
        select v into new_val from t134 where k = new_k;
        checksum = checksum + new_val;
      when 135 then
        insert into t135(v) values(val) returning k into new_k;
        select v into new_val from t135 where k = new_k;
        checksum = checksum + new_val;
      when 136 then
        insert into t136(v) values(val) returning k into new_k;
        select v into new_val from t136 where k = new_k;
        checksum = checksum + new_val;
      when 137 then
        insert into t137(v) values(val) returning k into new_k;
        select v into new_val from t137 where k = new_k;
        checksum = checksum + new_val;
      when 138 then
        insert into t138(v) values(val) returning k into new_k;
        select v into new_val from t138 where k = new_k;
        checksum = checksum + new_val;
      when 139 then
        insert into t139(v) values(val) returning k into new_k;
        select v into new_val from t139 where k = new_k;
        checksum = checksum + new_val;
      when 140 then
        insert into t140(v) values(val) returning k into new_k;
        select v into new_val from t140 where k = new_k;
        checksum = checksum + new_val;
      when 141 then
        insert into t141(v) values(val) returning k into new_k;
        select v into new_val from t141 where k = new_k;
        checksum = checksum + new_val;
      when 142 then
        insert into t142(v) values(val) returning k into new_k;
        select v into new_val from t142 where k = new_k;
        checksum = checksum + new_val;
      when 143 then
        insert into t143(v) values(val) returning k into new_k;
        select v into new_val from t143 where k = new_k;
        checksum = checksum + new_val;
      when 144 then
        insert into t144(v) values(val) returning k into new_k;
        select v into new_val from t144 where k = new_k;
        checksum = checksum + new_val;
      when 145 then
        insert into t145(v) values(val) returning k into new_k;
        select v into new_val from t145 where k = new_k;
        checksum = checksum + new_val;
      when 146 then
        insert into t146(v) values(val) returning k into new_k;
        select v into new_val from t146 where k = new_k;
        checksum = checksum + new_val;
      when 147 then
        insert into t147(v) values(val) returning k into new_k;
        select v into new_val from t147 where k = new_k;
        checksum = checksum + new_val;
      when 148 then
        insert into t148(v) values(val) returning k into new_k;
        select v into new_val from t148 where k = new_k;
        checksum = checksum + new_val;
      when 149 then
        insert into t149(v) values(val) returning k into new_k;
        select v into new_val from t149 where k = new_k;
        checksum = checksum + new_val;
      when 150 then
        insert into t150(v) values(val) returning k into new_k;
        select v into new_val from t150 where k = new_k;
        checksum = checksum + new_val;
      when 151 then
        insert into t151(v) values(val) returning k into new_k;
        select v into new_val from t151 where k = new_k;
        checksum = checksum + new_val;
      when 152 then
        insert into t152(v) values(val) returning k into new_k;
        select v into new_val from t152 where k = new_k;
        checksum = checksum + new_val;
      when 153 then
        insert into t153(v) values(val) returning k into new_k;
        select v into new_val from t153 where k = new_k;
        checksum = checksum + new_val;
      when 154 then
        insert into t154(v) values(val) returning k into new_k;
        select v into new_val from t154 where k = new_k;
        checksum = checksum + new_val;
      when 155 then
        insert into t155(v) values(val) returning k into new_k;
        select v into new_val from t155 where k = new_k;
        checksum = checksum + new_val;
      when 156 then
        insert into t156(v) values(val) returning k into new_k;
        select v into new_val from t156 where k = new_k;
        checksum = checksum + new_val;
      when 157 then
        insert into t157(v) values(val) returning k into new_k;
        select v into new_val from t157 where k = new_k;
        checksum = checksum + new_val;
      when 158 then
        insert into t158(v) values(val) returning k into new_k;
        select v into new_val from t158 where k = new_k;
        checksum = checksum + new_val;
      when 159 then
        insert into t159(v) values(val) returning k into new_k;
        select v into new_val from t159 where k = new_k;
        checksum = checksum + new_val;
      when 160 then
        insert into t160(v) values(val) returning k into new_k;
        select v into new_val from t160 where k = new_k;
        checksum = checksum + new_val;
      when 161 then
        insert into t161(v) values(val) returning k into new_k;
        select v into new_val from t161 where k = new_k;
        checksum = checksum + new_val;
      when 162 then
        insert into t162(v) values(val) returning k into new_k;
        select v into new_val from t162 where k = new_k;
        checksum = checksum + new_val;
      when 163 then
        insert into t163(v) values(val) returning k into new_k;
        select v into new_val from t163 where k = new_k;
        checksum = checksum + new_val;
      when 164 then
        insert into t164(v) values(val) returning k into new_k;
        select v into new_val from t164 where k = new_k;
        checksum = checksum + new_val;
      when 165 then
        insert into t165(v) values(val) returning k into new_k;
        select v into new_val from t165 where k = new_k;
        checksum = checksum + new_val;
      when 166 then
        insert into t166(v) values(val) returning k into new_k;
        select v into new_val from t166 where k = new_k;
        checksum = checksum + new_val;
      when 167 then
        insert into t167(v) values(val) returning k into new_k;
        select v into new_val from t167 where k = new_k;
        checksum = checksum + new_val;
      when 168 then
        insert into t168(v) values(val) returning k into new_k;
        select v into new_val from t168 where k = new_k;
        checksum = checksum + new_val;
      when 169 then
        insert into t169(v) values(val) returning k into new_k;
        select v into new_val from t169 where k = new_k;
        checksum = checksum + new_val;
      when 170 then
        insert into t170(v) values(val) returning k into new_k;
        select v into new_val from t170 where k = new_k;
        checksum = checksum + new_val;
      when 171 then
        insert into t171(v) values(val) returning k into new_k;
        select v into new_val from t171 where k = new_k;
        checksum = checksum + new_val;
      when 172 then
        insert into t172(v) values(val) returning k into new_k;
        select v into new_val from t172 where k = new_k;
        checksum = checksum + new_val;
      when 173 then
        insert into t173(v) values(val) returning k into new_k;
        select v into new_val from t173 where k = new_k;
        checksum = checksum + new_val;
      when 174 then
        insert into t174(v) values(val) returning k into new_k;
        select v into new_val from t174 where k = new_k;
        checksum = checksum + new_val;
      when 175 then
        insert into t175(v) values(val) returning k into new_k;
        select v into new_val from t175 where k = new_k;
        checksum = checksum + new_val;
      when 176 then
        insert into t176(v) values(val) returning k into new_k;
        select v into new_val from t176 where k = new_k;
        checksum = checksum + new_val;
      when 177 then
        insert into t177(v) values(val) returning k into new_k;
        select v into new_val from t177 where k = new_k;
        checksum = checksum + new_val;
      when 178 then
        insert into t178(v) values(val) returning k into new_k;
        select v into new_val from t178 where k = new_k;
        checksum = checksum + new_val;
      when 179 then
        insert into t179(v) values(val) returning k into new_k;
        select v into new_val from t179 where k = new_k;
        checksum = checksum + new_val;
      when 180 then
        insert into t180(v) values(val) returning k into new_k;
        select v into new_val from t180 where k = new_k;
        checksum = checksum + new_val;
      when 181 then
        insert into t181(v) values(val) returning k into new_k;
        select v into new_val from t181 where k = new_k;
        checksum = checksum + new_val;
      when 182 then
        insert into t182(v) values(val) returning k into new_k;
        select v into new_val from t182 where k = new_k;
        checksum = checksum + new_val;
      when 183 then
        insert into t183(v) values(val) returning k into new_k;
        select v into new_val from t183 where k = new_k;
        checksum = checksum + new_val;
      when 184 then
        insert into t184(v) values(val) returning k into new_k;
        select v into new_val from t184 where k = new_k;
        checksum = checksum + new_val;
      when 185 then
        insert into t185(v) values(val) returning k into new_k;
        select v into new_val from t185 where k = new_k;
        checksum = checksum + new_val;
      when 186 then
        insert into t186(v) values(val) returning k into new_k;
        select v into new_val from t186 where k = new_k;
        checksum = checksum + new_val;
      when 187 then
        insert into t187(v) values(val) returning k into new_k;
        select v into new_val from t187 where k = new_k;
        checksum = checksum + new_val;
      when 188 then
        insert into t188(v) values(val) returning k into new_k;
        select v into new_val from t188 where k = new_k;
        checksum = checksum + new_val;
      when 189 then
        insert into t189(v) values(val) returning k into new_k;
        select v into new_val from t189 where k = new_k;
        checksum = checksum + new_val;
      when 190 then
        insert into t190(v) values(val) returning k into new_k;
        select v into new_val from t190 where k = new_k;
        checksum = checksum + new_val;
      when 191 then
        insert into t191(v) values(val) returning k into new_k;
        select v into new_val from t191 where k = new_k;
        checksum = checksum + new_val;
      when 192 then
        insert into t192(v) values(val) returning k into new_k;
        select v into new_val from t192 where k = new_k;
        checksum = checksum + new_val;
      when 193 then
        insert into t193(v) values(val) returning k into new_k;
        select v into new_val from t193 where k = new_k;
        checksum = checksum + new_val;
      when 194 then
        insert into t194(v) values(val) returning k into new_k;
        select v into new_val from t194 where k = new_k;
        checksum = checksum + new_val;
      when 195 then
        insert into t195(v) values(val) returning k into new_k;
        select v into new_val from t195 where k = new_k;
        checksum = checksum + new_val;
      when 196 then
        insert into t196(v) values(val) returning k into new_k;
        select v into new_val from t196 where k = new_k;
        checksum = checksum + new_val;
      when 197 then
        insert into t197(v) values(val) returning k into new_k;
        select v into new_val from t197 where k = new_k;
        checksum = checksum + new_val;
      when 198 then
        insert into t198(v) values(val) returning k into new_k;
        select v into new_val from t198 where k = new_k;
        checksum = checksum + new_val;
      when 199 then
        insert into t199(v) values(val) returning k into new_k;
        select v into new_val from t199 where k = new_k;
        checksum = checksum + new_val;
      when 200 then
        insert into t200(v) values(val) returning k into new_k;
        select v into new_val from t200 where k = new_k;
        checksum = checksum + new_val;
      when 201 then
        insert into t201(v) values(val) returning k into new_k;
        select v into new_val from t201 where k = new_k;
        checksum = checksum + new_val;
      when 202 then
        insert into t202(v) values(val) returning k into new_k;
        select v into new_val from t202 where k = new_k;
        checksum = checksum + new_val;
      when 203 then
        insert into t203(v) values(val) returning k into new_k;
        select v into new_val from t203 where k = new_k;
        checksum = checksum + new_val;
      when 204 then
        insert into t204(v) values(val) returning k into new_k;
        select v into new_val from t204 where k = new_k;
        checksum = checksum + new_val;
      when 205 then
        insert into t205(v) values(val) returning k into new_k;
        select v into new_val from t205 where k = new_k;
        checksum = checksum + new_val;
      when 206 then
        insert into t206(v) values(val) returning k into new_k;
        select v into new_val from t206 where k = new_k;
        checksum = checksum + new_val;
      when 207 then
        insert into t207(v) values(val) returning k into new_k;
        select v into new_val from t207 where k = new_k;
        checksum = checksum + new_val;
      when 208 then
        insert into t208(v) values(val) returning k into new_k;
        select v into new_val from t208 where k = new_k;
        checksum = checksum + new_val;
      when 209 then
        insert into t209(v) values(val) returning k into new_k;
        select v into new_val from t209 where k = new_k;
        checksum = checksum + new_val;
      when 210 then
        insert into t210(v) values(val) returning k into new_k;
        select v into new_val from t210 where k = new_k;
        checksum = checksum + new_val;
      when 211 then
        insert into t211(v) values(val) returning k into new_k;
        select v into new_val from t211 where k = new_k;
        checksum = checksum + new_val;
      when 212 then
        insert into t212(v) values(val) returning k into new_k;
        select v into new_val from t212 where k = new_k;
        checksum = checksum + new_val;
      when 213 then
        insert into t213(v) values(val) returning k into new_k;
        select v into new_val from t213 where k = new_k;
        checksum = checksum + new_val;
      when 214 then
        insert into t214(v) values(val) returning k into new_k;
        select v into new_val from t214 where k = new_k;
        checksum = checksum + new_val;
      when 215 then
        insert into t215(v) values(val) returning k into new_k;
        select v into new_val from t215 where k = new_k;
        checksum = checksum + new_val;
      when 216 then
        insert into t216(v) values(val) returning k into new_k;
        select v into new_val from t216 where k = new_k;
        checksum = checksum + new_val;
      when 217 then
        insert into t217(v) values(val) returning k into new_k;
        select v into new_val from t217 where k = new_k;
        checksum = checksum + new_val;
      when 218 then
        insert into t218(v) values(val) returning k into new_k;
        select v into new_val from t218 where k = new_k;
        checksum = checksum + new_val;
      when 219 then
        insert into t219(v) values(val) returning k into new_k;
        select v into new_val from t219 where k = new_k;
        checksum = checksum + new_val;
      when 220 then
        insert into t220(v) values(val) returning k into new_k;
        select v into new_val from t220 where k = new_k;
        checksum = checksum + new_val;
      when 221 then
        insert into t221(v) values(val) returning k into new_k;
        select v into new_val from t221 where k = new_k;
        checksum = checksum + new_val;
      when 222 then
        insert into t222(v) values(val) returning k into new_k;
        select v into new_val from t222 where k = new_k;
        checksum = checksum + new_val;
      when 223 then
        insert into t223(v) values(val) returning k into new_k;
        select v into new_val from t223 where k = new_k;
        checksum = checksum + new_val;
      when 224 then
        insert into t224(v) values(val) returning k into new_k;
        select v into new_val from t224 where k = new_k;
        checksum = checksum + new_val;
      when 225 then
        insert into t225(v) values(val) returning k into new_k;
        select v into new_val from t225 where k = new_k;
        checksum = checksum + new_val;
      when 226 then
        insert into t226(v) values(val) returning k into new_k;
        select v into new_val from t226 where k = new_k;
        checksum = checksum + new_val;
      when 227 then
        insert into t227(v) values(val) returning k into new_k;
        select v into new_val from t227 where k = new_k;
        checksum = checksum + new_val;
      when 228 then
        insert into t228(v) values(val) returning k into new_k;
        select v into new_val from t228 where k = new_k;
        checksum = checksum + new_val;
      when 229 then
        insert into t229(v) values(val) returning k into new_k;
        select v into new_val from t229 where k = new_k;
        checksum = checksum + new_val;
      when 230 then
        insert into t230(v) values(val) returning k into new_k;
        select v into new_val from t230 where k = new_k;
        checksum = checksum + new_val;
      when 231 then
        insert into t231(v) values(val) returning k into new_k;
        select v into new_val from t231 where k = new_k;
        checksum = checksum + new_val;
      when 232 then
        insert into t232(v) values(val) returning k into new_k;
        select v into new_val from t232 where k = new_k;
        checksum = checksum + new_val;
      when 233 then
        insert into t233(v) values(val) returning k into new_k;
        select v into new_val from t233 where k = new_k;
        checksum = checksum + new_val;
      when 234 then
        insert into t234(v) values(val) returning k into new_k;
        select v into new_val from t234 where k = new_k;
        checksum = checksum + new_val;
      when 235 then
        insert into t235(v) values(val) returning k into new_k;
        select v into new_val from t235 where k = new_k;
        checksum = checksum + new_val;
      when 236 then
        insert into t236(v) values(val) returning k into new_k;
        select v into new_val from t236 where k = new_k;
        checksum = checksum + new_val;
      when 237 then
        insert into t237(v) values(val) returning k into new_k;
        select v into new_val from t237 where k = new_k;
        checksum = checksum + new_val;
      when 238 then
        insert into t238(v) values(val) returning k into new_k;
        select v into new_val from t238 where k = new_k;
        checksum = checksum + new_val;
      when 239 then
        insert into t239(v) values(val) returning k into new_k;
        select v into new_val from t239 where k = new_k;
        checksum = checksum + new_val;
      when 240 then
        insert into t240(v) values(val) returning k into new_k;
        select v into new_val from t240 where k = new_k;
        checksum = checksum + new_val;
      when 241 then
        insert into t241(v) values(val) returning k into new_k;
        select v into new_val from t241 where k = new_k;
        checksum = checksum + new_val;
      when 242 then
        insert into t242(v) values(val) returning k into new_k;
        select v into new_val from t242 where k = new_k;
        checksum = checksum + new_val;
      when 243 then
        insert into t243(v) values(val) returning k into new_k;
        select v into new_val from t243 where k = new_k;
        checksum = checksum + new_val;
      when 244 then
        insert into t244(v) values(val) returning k into new_k;
        select v into new_val from t244 where k = new_k;
        checksum = checksum + new_val;
      when 245 then
        insert into t245(v) values(val) returning k into new_k;
        select v into new_val from t245 where k = new_k;
        checksum = checksum + new_val;
      when 246 then
        insert into t246(v) values(val) returning k into new_k;
        select v into new_val from t246 where k = new_k;
        checksum = checksum + new_val;
      when 247 then
        insert into t247(v) values(val) returning k into new_k;
        select v into new_val from t247 where k = new_k;
        checksum = checksum + new_val;
      when 248 then
        insert into t248(v) values(val) returning k into new_k;
        select v into new_val from t248 where k = new_k;
        checksum = checksum + new_val;
      when 249 then
        insert into t249(v) values(val) returning k into new_k;
        select v into new_val from t249 where k = new_k;
        checksum = checksum + new_val;
      when 250 then
        insert into t250(v) values(val) returning k into new_k;
        select v into new_val from t250 where k = new_k;
        checksum = checksum + new_val;
      when 251 then
        insert into t251(v) values(val) returning k into new_k;
        select v into new_val from t251 where k = new_k;
        checksum = checksum + new_val;
      when 252 then
        insert into t252(v) values(val) returning k into new_k;
        select v into new_val from t252 where k = new_k;
        checksum = checksum + new_val;
      when 253 then
        insert into t253(v) values(val) returning k into new_k;
        select v into new_val from t253 where k = new_k;
        checksum = checksum + new_val;
      when 254 then
        insert into t254(v) values(val) returning k into new_k;
        select v into new_val from t254 where k = new_k;
        checksum = checksum + new_val;
      when 255 then
        insert into t255(v) values(val) returning k into new_k;
        select v into new_val from t255 where k = new_k;
        checksum = checksum + new_val;
      when 256 then
        insert into t256(v) values(val) returning k into new_k;
        select v into new_val from t256 where k = new_k;
        checksum = checksum + new_val;
      when 257 then
        insert into t257(v) values(val) returning k into new_k;
        select v into new_val from t257 where k = new_k;
        checksum = checksum + new_val;
      when 258 then
        insert into t258(v) values(val) returning k into new_k;
        select v into new_val from t258 where k = new_k;
        checksum = checksum + new_val;
      when 259 then
        insert into t259(v) values(val) returning k into new_k;
        select v into new_val from t259 where k = new_k;
        checksum = checksum + new_val;
      when 260 then
        insert into t260(v) values(val) returning k into new_k;
        select v into new_val from t260 where k = new_k;
        checksum = checksum + new_val;
      when 261 then
        insert into t261(v) values(val) returning k into new_k;
        select v into new_val from t261 where k = new_k;
        checksum = checksum + new_val;
      when 262 then
        insert into t262(v) values(val) returning k into new_k;
        select v into new_val from t262 where k = new_k;
        checksum = checksum + new_val;
      when 263 then
        insert into t263(v) values(val) returning k into new_k;
        select v into new_val from t263 where k = new_k;
        checksum = checksum + new_val;
      when 264 then
        insert into t264(v) values(val) returning k into new_k;
        select v into new_val from t264 where k = new_k;
        checksum = checksum + new_val;
      when 265 then
        insert into t265(v) values(val) returning k into new_k;
        select v into new_val from t265 where k = new_k;
        checksum = checksum + new_val;
      when 266 then
        insert into t266(v) values(val) returning k into new_k;
        select v into new_val from t266 where k = new_k;
        checksum = checksum + new_val;
      when 267 then
        insert into t267(v) values(val) returning k into new_k;
        select v into new_val from t267 where k = new_k;
        checksum = checksum + new_val;
      when 268 then
        insert into t268(v) values(val) returning k into new_k;
        select v into new_val from t268 where k = new_k;
        checksum = checksum + new_val;
      when 269 then
        insert into t269(v) values(val) returning k into new_k;
        select v into new_val from t269 where k = new_k;
        checksum = checksum + new_val;
      when 270 then
        insert into t270(v) values(val) returning k into new_k;
        select v into new_val from t270 where k = new_k;
        checksum = checksum + new_val;
      when 271 then
        insert into t271(v) values(val) returning k into new_k;
        select v into new_val from t271 where k = new_k;
        checksum = checksum + new_val;
      when 272 then
        insert into t272(v) values(val) returning k into new_k;
        select v into new_val from t272 where k = new_k;
        checksum = checksum + new_val;
      when 273 then
        insert into t273(v) values(val) returning k into new_k;
        select v into new_val from t273 where k = new_k;
        checksum = checksum + new_val;
      when 274 then
        insert into t274(v) values(val) returning k into new_k;
        select v into new_val from t274 where k = new_k;
        checksum = checksum + new_val;
      when 275 then
        insert into t275(v) values(val) returning k into new_k;
        select v into new_val from t275 where k = new_k;
        checksum = checksum + new_val;
      when 276 then
        insert into t276(v) values(val) returning k into new_k;
        select v into new_val from t276 where k = new_k;
        checksum = checksum + new_val;
      when 277 then
        insert into t277(v) values(val) returning k into new_k;
        select v into new_val from t277 where k = new_k;
        checksum = checksum + new_val;
      when 278 then
        insert into t278(v) values(val) returning k into new_k;
        select v into new_val from t278 where k = new_k;
        checksum = checksum + new_val;
      when 279 then
        insert into t279(v) values(val) returning k into new_k;
        select v into new_val from t279 where k = new_k;
        checksum = checksum + new_val;
      when 280 then
        insert into t280(v) values(val) returning k into new_k;
        select v into new_val from t280 where k = new_k;
        checksum = checksum + new_val;
      when 281 then
        insert into t281(v) values(val) returning k into new_k;
        select v into new_val from t281 where k = new_k;
        checksum = checksum + new_val;
      when 282 then
        insert into t282(v) values(val) returning k into new_k;
        select v into new_val from t282 where k = new_k;
        checksum = checksum + new_val;
      when 283 then
        insert into t283(v) values(val) returning k into new_k;
        select v into new_val from t283 where k = new_k;
        checksum = checksum + new_val;
      when 284 then
        insert into t284(v) values(val) returning k into new_k;
        select v into new_val from t284 where k = new_k;
        checksum = checksum + new_val;
      when 285 then
        insert into t285(v) values(val) returning k into new_k;
        select v into new_val from t285 where k = new_k;
        checksum = checksum + new_val;
      when 286 then
        insert into t286(v) values(val) returning k into new_k;
        select v into new_val from t286 where k = new_k;
        checksum = checksum + new_val;
      when 287 then
        insert into t287(v) values(val) returning k into new_k;
        select v into new_val from t287 where k = new_k;
        checksum = checksum + new_val;
      when 288 then
        insert into t288(v) values(val) returning k into new_k;
        select v into new_val from t288 where k = new_k;
        checksum = checksum + new_val;
      when 289 then
        insert into t289(v) values(val) returning k into new_k;
        select v into new_val from t289 where k = new_k;
        checksum = checksum + new_val;
      when 290 then
        insert into t290(v) values(val) returning k into new_k;
        select v into new_val from t290 where k = new_k;
        checksum = checksum + new_val;
      when 291 then
        insert into t291(v) values(val) returning k into new_k;
        select v into new_val from t291 where k = new_k;
        checksum = checksum + new_val;
      when 292 then
        insert into t292(v) values(val) returning k into new_k;
        select v into new_val from t292 where k = new_k;
        checksum = checksum + new_val;
      when 293 then
        insert into t293(v) values(val) returning k into new_k;
        select v into new_val from t293 where k = new_k;
        checksum = checksum + new_val;
      when 294 then
        insert into t294(v) values(val) returning k into new_k;
        select v into new_val from t294 where k = new_k;
        checksum = checksum + new_val;
      when 295 then
        insert into t295(v) values(val) returning k into new_k;
        select v into new_val from t295 where k = new_k;
        checksum = checksum + new_val;
      when 296 then
        insert into t296(v) values(val) returning k into new_k;
        select v into new_val from t296 where k = new_k;
        checksum = checksum + new_val;
      when 297 then
        insert into t297(v) values(val) returning k into new_k;
        select v into new_val from t297 where k = new_k;
        checksum = checksum + new_val;
      when 298 then
        insert into t298(v) values(val) returning k into new_k;
        select v into new_val from t298 where k = new_k;
        checksum = checksum + new_val;
      when 299 then
        insert into t299(v) values(val) returning k into new_k;
        select v into new_val from t299 where k = new_k;
        checksum = checksum + new_val;
      when 300 then
        insert into t300(v) values(val) returning k into new_k;
        select v into new_val from t300 where k = new_k;
        checksum = checksum + new_val;
      when 301 then
        insert into t301(v) values(val) returning k into new_k;
        select v into new_val from t301 where k = new_k;
        checksum = checksum + new_val;
      when 302 then
        insert into t302(v) values(val) returning k into new_k;
        select v into new_val from t302 where k = new_k;
        checksum = checksum + new_val;
      when 303 then
        insert into t303(v) values(val) returning k into new_k;
        select v into new_val from t303 where k = new_k;
        checksum = checksum + new_val;
      when 304 then
        insert into t304(v) values(val) returning k into new_k;
        select v into new_val from t304 where k = new_k;
        checksum = checksum + new_val;
      when 305 then
        insert into t305(v) values(val) returning k into new_k;
        select v into new_val from t305 where k = new_k;
        checksum = checksum + new_val;
      when 306 then
        insert into t306(v) values(val) returning k into new_k;
        select v into new_val from t306 where k = new_k;
        checksum = checksum + new_val;
      when 307 then
        insert into t307(v) values(val) returning k into new_k;
        select v into new_val from t307 where k = new_k;
        checksum = checksum + new_val;
      when 308 then
        insert into t308(v) values(val) returning k into new_k;
        select v into new_val from t308 where k = new_k;
        checksum = checksum + new_val;
      when 309 then
        insert into t309(v) values(val) returning k into new_k;
        select v into new_val from t309 where k = new_k;
        checksum = checksum + new_val;
      when 310 then
        insert into t310(v) values(val) returning k into new_k;
        select v into new_val from t310 where k = new_k;
        checksum = checksum + new_val;
      when 311 then
        insert into t311(v) values(val) returning k into new_k;
        select v into new_val from t311 where k = new_k;
        checksum = checksum + new_val;
      when 312 then
        insert into t312(v) values(val) returning k into new_k;
        select v into new_val from t312 where k = new_k;
        checksum = checksum + new_val;
      when 313 then
        insert into t313(v) values(val) returning k into new_k;
        select v into new_val from t313 where k = new_k;
        checksum = checksum + new_val;
      when 314 then
        insert into t314(v) values(val) returning k into new_k;
        select v into new_val from t314 where k = new_k;
        checksum = checksum + new_val;
      when 315 then
        insert into t315(v) values(val) returning k into new_k;
        select v into new_val from t315 where k = new_k;
        checksum = checksum + new_val;
      when 316 then
        insert into t316(v) values(val) returning k into new_k;
        select v into new_val from t316 where k = new_k;
        checksum = checksum + new_val;
      when 317 then
        insert into t317(v) values(val) returning k into new_k;
        select v into new_val from t317 where k = new_k;
        checksum = checksum + new_val;
      when 318 then
        insert into t318(v) values(val) returning k into new_k;
        select v into new_val from t318 where k = new_k;
        checksum = checksum + new_val;
      when 319 then
        insert into t319(v) values(val) returning k into new_k;
        select v into new_val from t319 where k = new_k;
        checksum = checksum + new_val;
      when 320 then
        insert into t320(v) values(val) returning k into new_k;
        select v into new_val from t320 where k = new_k;
        checksum = checksum + new_val;
      when 321 then
        insert into t321(v) values(val) returning k into new_k;
        select v into new_val from t321 where k = new_k;
        checksum = checksum + new_val;
      when 322 then
        insert into t322(v) values(val) returning k into new_k;
        select v into new_val from t322 where k = new_k;
        checksum = checksum + new_val;
      when 323 then
        insert into t323(v) values(val) returning k into new_k;
        select v into new_val from t323 where k = new_k;
        checksum = checksum + new_val;
      when 324 then
        insert into t324(v) values(val) returning k into new_k;
        select v into new_val from t324 where k = new_k;
        checksum = checksum + new_val;
      when 325 then
        insert into t325(v) values(val) returning k into new_k;
        select v into new_val from t325 where k = new_k;
        checksum = checksum + new_val;
      when 326 then
        insert into t326(v) values(val) returning k into new_k;
        select v into new_val from t326 where k = new_k;
        checksum = checksum + new_val;
      when 327 then
        insert into t327(v) values(val) returning k into new_k;
        select v into new_val from t327 where k = new_k;
        checksum = checksum + new_val;
      when 328 then
        insert into t328(v) values(val) returning k into new_k;
        select v into new_val from t328 where k = new_k;
        checksum = checksum + new_val;
      when 329 then
        insert into t329(v) values(val) returning k into new_k;
        select v into new_val from t329 where k = new_k;
        checksum = checksum + new_val;
      when 330 then
        insert into t330(v) values(val) returning k into new_k;
        select v into new_val from t330 where k = new_k;
        checksum = checksum + new_val;
      when 331 then
        insert into t331(v) values(val) returning k into new_k;
        select v into new_val from t331 where k = new_k;
        checksum = checksum + new_val;
      when 332 then
        insert into t332(v) values(val) returning k into new_k;
        select v into new_val from t332 where k = new_k;
        checksum = checksum + new_val;
      when 333 then
        insert into t333(v) values(val) returning k into new_k;
        select v into new_val from t333 where k = new_k;
        checksum = checksum + new_val;
      when 334 then
        insert into t334(v) values(val) returning k into new_k;
        select v into new_val from t334 where k = new_k;
        checksum = checksum + new_val;
      when 335 then
        insert into t335(v) values(val) returning k into new_k;
        select v into new_val from t335 where k = new_k;
        checksum = checksum + new_val;
      when 336 then
        insert into t336(v) values(val) returning k into new_k;
        select v into new_val from t336 where k = new_k;
        checksum = checksum + new_val;
      when 337 then
        insert into t337(v) values(val) returning k into new_k;
        select v into new_val from t337 where k = new_k;
        checksum = checksum + new_val;
      when 338 then
        insert into t338(v) values(val) returning k into new_k;
        select v into new_val from t338 where k = new_k;
        checksum = checksum + new_val;
      when 339 then
        insert into t339(v) values(val) returning k into new_k;
        select v into new_val from t339 where k = new_k;
        checksum = checksum + new_val;
      when 340 then
        insert into t340(v) values(val) returning k into new_k;
        select v into new_val from t340 where k = new_k;
        checksum = checksum + new_val;
      when 341 then
        insert into t341(v) values(val) returning k into new_k;
        select v into new_val from t341 where k = new_k;
        checksum = checksum + new_val;
      when 342 then
        insert into t342(v) values(val) returning k into new_k;
        select v into new_val from t342 where k = new_k;
        checksum = checksum + new_val;
      when 343 then
        insert into t343(v) values(val) returning k into new_k;
        select v into new_val from t343 where k = new_k;
        checksum = checksum + new_val;
      when 344 then
        insert into t344(v) values(val) returning k into new_k;
        select v into new_val from t344 where k = new_k;
        checksum = checksum + new_val;
      when 345 then
        insert into t345(v) values(val) returning k into new_k;
        select v into new_val from t345 where k = new_k;
        checksum = checksum + new_val;
      when 346 then
        insert into t346(v) values(val) returning k into new_k;
        select v into new_val from t346 where k = new_k;
        checksum = checksum + new_val;
      when 347 then
        insert into t347(v) values(val) returning k into new_k;
        select v into new_val from t347 where k = new_k;
        checksum = checksum + new_val;
      when 348 then
        insert into t348(v) values(val) returning k into new_k;
        select v into new_val from t348 where k = new_k;
        checksum = checksum + new_val;
      when 349 then
        insert into t349(v) values(val) returning k into new_k;
        select v into new_val from t349 where k = new_k;
        checksum = checksum + new_val;
      when 350 then
        insert into t350(v) values(val) returning k into new_k;
        select v into new_val from t350 where k = new_k;
        checksum = checksum + new_val;
      when 351 then
        insert into t351(v) values(val) returning k into new_k;
        select v into new_val from t351 where k = new_k;
        checksum = checksum + new_val;
      when 352 then
        insert into t352(v) values(val) returning k into new_k;
        select v into new_val from t352 where k = new_k;
        checksum = checksum + new_val;
      when 353 then
        insert into t353(v) values(val) returning k into new_k;
        select v into new_val from t353 where k = new_k;
        checksum = checksum + new_val;
      when 354 then
        insert into t354(v) values(val) returning k into new_k;
        select v into new_val from t354 where k = new_k;
        checksum = checksum + new_val;
      when 355 then
        insert into t355(v) values(val) returning k into new_k;
        select v into new_val from t355 where k = new_k;
        checksum = checksum + new_val;
      when 356 then
        insert into t356(v) values(val) returning k into new_k;
        select v into new_val from t356 where k = new_k;
        checksum = checksum + new_val;
      when 357 then
        insert into t357(v) values(val) returning k into new_k;
        select v into new_val from t357 where k = new_k;
        checksum = checksum + new_val;
      when 358 then
        insert into t358(v) values(val) returning k into new_k;
        select v into new_val from t358 where k = new_k;
        checksum = checksum + new_val;
      when 359 then
        insert into t359(v) values(val) returning k into new_k;
        select v into new_val from t359 where k = new_k;
        checksum = checksum + new_val;
      when 360 then
        insert into t360(v) values(val) returning k into new_k;
        select v into new_val from t360 where k = new_k;
        checksum = checksum + new_val;
      when 361 then
        insert into t361(v) values(val) returning k into new_k;
        select v into new_val from t361 where k = new_k;
        checksum = checksum + new_val;
      when 362 then
        insert into t362(v) values(val) returning k into new_k;
        select v into new_val from t362 where k = new_k;
        checksum = checksum + new_val;
      when 363 then
        insert into t363(v) values(val) returning k into new_k;
        select v into new_val from t363 where k = new_k;
        checksum = checksum + new_val;
      when 364 then
        insert into t364(v) values(val) returning k into new_k;
        select v into new_val from t364 where k = new_k;
        checksum = checksum + new_val;
      when 365 then
        insert into t365(v) values(val) returning k into new_k;
        select v into new_val from t365 where k = new_k;
        checksum = checksum + new_val;
      when 366 then
        insert into t366(v) values(val) returning k into new_k;
        select v into new_val from t366 where k = new_k;
        checksum = checksum + new_val;
      when 367 then
        insert into t367(v) values(val) returning k into new_k;
        select v into new_val from t367 where k = new_k;
        checksum = checksum + new_val;
      when 368 then
        insert into t368(v) values(val) returning k into new_k;
        select v into new_val from t368 where k = new_k;
        checksum = checksum + new_val;
      when 369 then
        insert into t369(v) values(val) returning k into new_k;
        select v into new_val from t369 where k = new_k;
        checksum = checksum + new_val;
      when 370 then
        insert into t370(v) values(val) returning k into new_k;
        select v into new_val from t370 where k = new_k;
        checksum = checksum + new_val;
      when 371 then
        insert into t371(v) values(val) returning k into new_k;
        select v into new_val from t371 where k = new_k;
        checksum = checksum + new_val;
      when 372 then
        insert into t372(v) values(val) returning k into new_k;
        select v into new_val from t372 where k = new_k;
        checksum = checksum + new_val;
      when 373 then
        insert into t373(v) values(val) returning k into new_k;
        select v into new_val from t373 where k = new_k;
        checksum = checksum + new_val;
      when 374 then
        insert into t374(v) values(val) returning k into new_k;
        select v into new_val from t374 where k = new_k;
        checksum = checksum + new_val;
      when 375 then
        insert into t375(v) values(val) returning k into new_k;
        select v into new_val from t375 where k = new_k;
        checksum = checksum + new_val;
      when 376 then
        insert into t376(v) values(val) returning k into new_k;
        select v into new_val from t376 where k = new_k;
        checksum = checksum + new_val;
      when 377 then
        insert into t377(v) values(val) returning k into new_k;
        select v into new_val from t377 where k = new_k;
        checksum = checksum + new_val;
      when 378 then
        insert into t378(v) values(val) returning k into new_k;
        select v into new_val from t378 where k = new_k;
        checksum = checksum + new_val;
      when 379 then
        insert into t379(v) values(val) returning k into new_k;
        select v into new_val from t379 where k = new_k;
        checksum = checksum + new_val;
      when 380 then
        insert into t380(v) values(val) returning k into new_k;
        select v into new_val from t380 where k = new_k;
        checksum = checksum + new_val;
      when 381 then
        insert into t381(v) values(val) returning k into new_k;
        select v into new_val from t381 where k = new_k;
        checksum = checksum + new_val;
      when 382 then
        insert into t382(v) values(val) returning k into new_k;
        select v into new_val from t382 where k = new_k;
        checksum = checksum + new_val;
      when 383 then
        insert into t383(v) values(val) returning k into new_k;
        select v into new_val from t383 where k = new_k;
        checksum = checksum + new_val;
      when 384 then
        insert into t384(v) values(val) returning k into new_k;
        select v into new_val from t384 where k = new_k;
        checksum = checksum + new_val;
      when 385 then
        insert into t385(v) values(val) returning k into new_k;
        select v into new_val from t385 where k = new_k;
        checksum = checksum + new_val;
      when 386 then
        insert into t386(v) values(val) returning k into new_k;
        select v into new_val from t386 where k = new_k;
        checksum = checksum + new_val;
      when 387 then
        insert into t387(v) values(val) returning k into new_k;
        select v into new_val from t387 where k = new_k;
        checksum = checksum + new_val;
      when 388 then
        insert into t388(v) values(val) returning k into new_k;
        select v into new_val from t388 where k = new_k;
        checksum = checksum + new_val;
      when 389 then
        insert into t389(v) values(val) returning k into new_k;
        select v into new_val from t389 where k = new_k;
        checksum = checksum + new_val;
      when 390 then
        insert into t390(v) values(val) returning k into new_k;
        select v into new_val from t390 where k = new_k;
        checksum = checksum + new_val;
      when 391 then
        insert into t391(v) values(val) returning k into new_k;
        select v into new_val from t391 where k = new_k;
        checksum = checksum + new_val;
      when 392 then
        insert into t392(v) values(val) returning k into new_k;
        select v into new_val from t392 where k = new_k;
        checksum = checksum + new_val;
      when 393 then
        insert into t393(v) values(val) returning k into new_k;
        select v into new_val from t393 where k = new_k;
        checksum = checksum + new_val;
      when 394 then
        insert into t394(v) values(val) returning k into new_k;
        select v into new_val from t394 where k = new_k;
        checksum = checksum + new_val;
      when 395 then
        insert into t395(v) values(val) returning k into new_k;
        select v into new_val from t395 where k = new_k;
        checksum = checksum + new_val;
      when 396 then
        insert into t396(v) values(val) returning k into new_k;
        select v into new_val from t396 where k = new_k;
        checksum = checksum + new_val;
      when 397 then
        insert into t397(v) values(val) returning k into new_k;
        select v into new_val from t397 where k = new_k;
        checksum = checksum + new_val;
      when 398 then
        insert into t398(v) values(val) returning k into new_k;
        select v into new_val from t398 where k = new_k;
        checksum = checksum + new_val;
      when 399 then
        insert into t399(v) values(val) returning k into new_k;
        select v into new_val from t399 where k = new_k;
        checksum = checksum + new_val;
      when 400 then
        insert into t400(v) values(val) returning k into new_k;
        select v into new_val from t400 where k = new_k;
        checksum = checksum + new_val;
      when 401 then
        insert into t401(v) values(val) returning k into new_k;
        select v into new_val from t401 where k = new_k;
        checksum = checksum + new_val;
      when 402 then
        insert into t402(v) values(val) returning k into new_k;
        select v into new_val from t402 where k = new_k;
        checksum = checksum + new_val;
      when 403 then
        insert into t403(v) values(val) returning k into new_k;
        select v into new_val from t403 where k = new_k;
        checksum = checksum + new_val;
      when 404 then
        insert into t404(v) values(val) returning k into new_k;
        select v into new_val from t404 where k = new_k;
        checksum = checksum + new_val;
      when 405 then
        insert into t405(v) values(val) returning k into new_k;
        select v into new_val from t405 where k = new_k;
        checksum = checksum + new_val;
      when 406 then
        insert into t406(v) values(val) returning k into new_k;
        select v into new_val from t406 where k = new_k;
        checksum = checksum + new_val;
      when 407 then
        insert into t407(v) values(val) returning k into new_k;
        select v into new_val from t407 where k = new_k;
        checksum = checksum + new_val;
      when 408 then
        insert into t408(v) values(val) returning k into new_k;
        select v into new_val from t408 where k = new_k;
        checksum = checksum + new_val;
      when 409 then
        insert into t409(v) values(val) returning k into new_k;
        select v into new_val from t409 where k = new_k;
        checksum = checksum + new_val;
      when 410 then
        insert into t410(v) values(val) returning k into new_k;
        select v into new_val from t410 where k = new_k;
        checksum = checksum + new_val;
      when 411 then
        insert into t411(v) values(val) returning k into new_k;
        select v into new_val from t411 where k = new_k;
        checksum = checksum + new_val;
      when 412 then
        insert into t412(v) values(val) returning k into new_k;
        select v into new_val from t412 where k = new_k;
        checksum = checksum + new_val;
      when 413 then
        insert into t413(v) values(val) returning k into new_k;
        select v into new_val from t413 where k = new_k;
        checksum = checksum + new_val;
      when 414 then
        insert into t414(v) values(val) returning k into new_k;
        select v into new_val from t414 where k = new_k;
        checksum = checksum + new_val;
      when 415 then
        insert into t415(v) values(val) returning k into new_k;
        select v into new_val from t415 where k = new_k;
        checksum = checksum + new_val;
      when 416 then
        insert into t416(v) values(val) returning k into new_k;
        select v into new_val from t416 where k = new_k;
        checksum = checksum + new_val;
      when 417 then
        insert into t417(v) values(val) returning k into new_k;
        select v into new_val from t417 where k = new_k;
        checksum = checksum + new_val;
      when 418 then
        insert into t418(v) values(val) returning k into new_k;
        select v into new_val from t418 where k = new_k;
        checksum = checksum + new_val;
      when 419 then
        insert into t419(v) values(val) returning k into new_k;
        select v into new_val from t419 where k = new_k;
        checksum = checksum + new_val;
      when 420 then
        insert into t420(v) values(val) returning k into new_k;
        select v into new_val from t420 where k = new_k;
        checksum = checksum + new_val;
      when 421 then
        insert into t421(v) values(val) returning k into new_k;
        select v into new_val from t421 where k = new_k;
        checksum = checksum + new_val;
      when 422 then
        insert into t422(v) values(val) returning k into new_k;
        select v into new_val from t422 where k = new_k;
        checksum = checksum + new_val;
      when 423 then
        insert into t423(v) values(val) returning k into new_k;
        select v into new_val from t423 where k = new_k;
        checksum = checksum + new_val;
      when 424 then
        insert into t424(v) values(val) returning k into new_k;
        select v into new_val from t424 where k = new_k;
        checksum = checksum + new_val;
      when 425 then
        insert into t425(v) values(val) returning k into new_k;
        select v into new_val from t425 where k = new_k;
        checksum = checksum + new_val;
      when 426 then
        insert into t426(v) values(val) returning k into new_k;
        select v into new_val from t426 where k = new_k;
        checksum = checksum + new_val;
      when 427 then
        insert into t427(v) values(val) returning k into new_k;
        select v into new_val from t427 where k = new_k;
        checksum = checksum + new_val;
      when 428 then
        insert into t428(v) values(val) returning k into new_k;
        select v into new_val from t428 where k = new_k;
        checksum = checksum + new_val;
      when 429 then
        insert into t429(v) values(val) returning k into new_k;
        select v into new_val from t429 where k = new_k;
        checksum = checksum + new_val;
      when 430 then
        insert into t430(v) values(val) returning k into new_k;
        select v into new_val from t430 where k = new_k;
        checksum = checksum + new_val;
      when 431 then
        insert into t431(v) values(val) returning k into new_k;
        select v into new_val from t431 where k = new_k;
        checksum = checksum + new_val;
      when 432 then
        insert into t432(v) values(val) returning k into new_k;
        select v into new_val from t432 where k = new_k;
        checksum = checksum + new_val;
      when 433 then
        insert into t433(v) values(val) returning k into new_k;
        select v into new_val from t433 where k = new_k;
        checksum = checksum + new_val;
      when 434 then
        insert into t434(v) values(val) returning k into new_k;
        select v into new_val from t434 where k = new_k;
        checksum = checksum + new_val;
      when 435 then
        insert into t435(v) values(val) returning k into new_k;
        select v into new_val from t435 where k = new_k;
        checksum = checksum + new_val;
      when 436 then
        insert into t436(v) values(val) returning k into new_k;
        select v into new_val from t436 where k = new_k;
        checksum = checksum + new_val;
      when 437 then
        insert into t437(v) values(val) returning k into new_k;
        select v into new_val from t437 where k = new_k;
        checksum = checksum + new_val;
      when 438 then
        insert into t438(v) values(val) returning k into new_k;
        select v into new_val from t438 where k = new_k;
        checksum = checksum + new_val;
      when 439 then
        insert into t439(v) values(val) returning k into new_k;
        select v into new_val from t439 where k = new_k;
        checksum = checksum + new_val;
      when 440 then
        insert into t440(v) values(val) returning k into new_k;
        select v into new_val from t440 where k = new_k;
        checksum = checksum + new_val;
      when 441 then
        insert into t441(v) values(val) returning k into new_k;
        select v into new_val from t441 where k = new_k;
        checksum = checksum + new_val;
      when 442 then
        insert into t442(v) values(val) returning k into new_k;
        select v into new_val from t442 where k = new_k;
        checksum = checksum + new_val;
      when 443 then
        insert into t443(v) values(val) returning k into new_k;
        select v into new_val from t443 where k = new_k;
        checksum = checksum + new_val;
      when 444 then
        insert into t444(v) values(val) returning k into new_k;
        select v into new_val from t444 where k = new_k;
        checksum = checksum + new_val;
      when 445 then
        insert into t445(v) values(val) returning k into new_k;
        select v into new_val from t445 where k = new_k;
        checksum = checksum + new_val;
      when 446 then
        insert into t446(v) values(val) returning k into new_k;
        select v into new_val from t446 where k = new_k;
        checksum = checksum + new_val;
      when 447 then
        insert into t447(v) values(val) returning k into new_k;
        select v into new_val from t447 where k = new_k;
        checksum = checksum + new_val;
      when 448 then
        insert into t448(v) values(val) returning k into new_k;
        select v into new_val from t448 where k = new_k;
        checksum = checksum + new_val;
      when 449 then
        insert into t449(v) values(val) returning k into new_k;
        select v into new_val from t449 where k = new_k;
        checksum = checksum + new_val;
      when 450 then
        insert into t450(v) values(val) returning k into new_k;
        select v into new_val from t450 where k = new_k;
        checksum = checksum + new_val;
      when 451 then
        insert into t451(v) values(val) returning k into new_k;
        select v into new_val from t451 where k = new_k;
        checksum = checksum + new_val;
      when 452 then
        insert into t452(v) values(val) returning k into new_k;
        select v into new_val from t452 where k = new_k;
        checksum = checksum + new_val;
      when 453 then
        insert into t453(v) values(val) returning k into new_k;
        select v into new_val from t453 where k = new_k;
        checksum = checksum + new_val;
      when 454 then
        insert into t454(v) values(val) returning k into new_k;
        select v into new_val from t454 where k = new_k;
        checksum = checksum + new_val;
      when 455 then
        insert into t455(v) values(val) returning k into new_k;
        select v into new_val from t455 where k = new_k;
        checksum = checksum + new_val;
      when 456 then
        insert into t456(v) values(val) returning k into new_k;
        select v into new_val from t456 where k = new_k;
        checksum = checksum + new_val;
      when 457 then
        insert into t457(v) values(val) returning k into new_k;
        select v into new_val from t457 where k = new_k;
        checksum = checksum + new_val;
      when 458 then
        insert into t458(v) values(val) returning k into new_k;
        select v into new_val from t458 where k = new_k;
        checksum = checksum + new_val;
      when 459 then
        insert into t459(v) values(val) returning k into new_k;
        select v into new_val from t459 where k = new_k;
        checksum = checksum + new_val;
      when 460 then
        insert into t460(v) values(val) returning k into new_k;
        select v into new_val from t460 where k = new_k;
        checksum = checksum + new_val;
      when 461 then
        insert into t461(v) values(val) returning k into new_k;
        select v into new_val from t461 where k = new_k;
        checksum = checksum + new_val;
      when 462 then
        insert into t462(v) values(val) returning k into new_k;
        select v into new_val from t462 where k = new_k;
        checksum = checksum + new_val;
      when 463 then
        insert into t463(v) values(val) returning k into new_k;
        select v into new_val from t463 where k = new_k;
        checksum = checksum + new_val;
      when 464 then
        insert into t464(v) values(val) returning k into new_k;
        select v into new_val from t464 where k = new_k;
        checksum = checksum + new_val;
      when 465 then
        insert into t465(v) values(val) returning k into new_k;
        select v into new_val from t465 where k = new_k;
        checksum = checksum + new_val;
      when 466 then
        insert into t466(v) values(val) returning k into new_k;
        select v into new_val from t466 where k = new_k;
        checksum = checksum + new_val;
      when 467 then
        insert into t467(v) values(val) returning k into new_k;
        select v into new_val from t467 where k = new_k;
        checksum = checksum + new_val;
      when 468 then
        insert into t468(v) values(val) returning k into new_k;
        select v into new_val from t468 where k = new_k;
        checksum = checksum + new_val;
      when 469 then
        insert into t469(v) values(val) returning k into new_k;
        select v into new_val from t469 where k = new_k;
        checksum = checksum + new_val;
      when 470 then
        insert into t470(v) values(val) returning k into new_k;
        select v into new_val from t470 where k = new_k;
        checksum = checksum + new_val;
      when 471 then
        insert into t471(v) values(val) returning k into new_k;
        select v into new_val from t471 where k = new_k;
        checksum = checksum + new_val;
      when 472 then
        insert into t472(v) values(val) returning k into new_k;
        select v into new_val from t472 where k = new_k;
        checksum = checksum + new_val;
      when 473 then
        insert into t473(v) values(val) returning k into new_k;
        select v into new_val from t473 where k = new_k;
        checksum = checksum + new_val;
      when 474 then
        insert into t474(v) values(val) returning k into new_k;
        select v into new_val from t474 where k = new_k;
        checksum = checksum + new_val;
      when 475 then
        insert into t475(v) values(val) returning k into new_k;
        select v into new_val from t475 where k = new_k;
        checksum = checksum + new_val;
      when 476 then
        insert into t476(v) values(val) returning k into new_k;
        select v into new_val from t476 where k = new_k;
        checksum = checksum + new_val;
      when 477 then
        insert into t477(v) values(val) returning k into new_k;
        select v into new_val from t477 where k = new_k;
        checksum = checksum + new_val;
      when 478 then
        insert into t478(v) values(val) returning k into new_k;
        select v into new_val from t478 where k = new_k;
        checksum = checksum + new_val;
      when 479 then
        insert into t479(v) values(val) returning k into new_k;
        select v into new_val from t479 where k = new_k;
        checksum = checksum + new_val;
      when 480 then
        insert into t480(v) values(val) returning k into new_k;
        select v into new_val from t480 where k = new_k;
        checksum = checksum + new_val;
      when 481 then
        insert into t481(v) values(val) returning k into new_k;
        select v into new_val from t481 where k = new_k;
        checksum = checksum + new_val;
      when 482 then
        insert into t482(v) values(val) returning k into new_k;
        select v into new_val from t482 where k = new_k;
        checksum = checksum + new_val;
      when 483 then
        insert into t483(v) values(val) returning k into new_k;
        select v into new_val from t483 where k = new_k;
        checksum = checksum + new_val;
      when 484 then
        insert into t484(v) values(val) returning k into new_k;
        select v into new_val from t484 where k = new_k;
        checksum = checksum + new_val;
      when 485 then
        insert into t485(v) values(val) returning k into new_k;
        select v into new_val from t485 where k = new_k;
        checksum = checksum + new_val;
      when 486 then
        insert into t486(v) values(val) returning k into new_k;
        select v into new_val from t486 where k = new_k;
        checksum = checksum + new_val;
      when 487 then
        insert into t487(v) values(val) returning k into new_k;
        select v into new_val from t487 where k = new_k;
        checksum = checksum + new_val;
      when 488 then
        insert into t488(v) values(val) returning k into new_k;
        select v into new_val from t488 where k = new_k;
        checksum = checksum + new_val;
      when 489 then
        insert into t489(v) values(val) returning k into new_k;
        select v into new_val from t489 where k = new_k;
        checksum = checksum + new_val;
      when 490 then
        insert into t490(v) values(val) returning k into new_k;
        select v into new_val from t490 where k = new_k;
        checksum = checksum + new_val;
      when 491 then
        insert into t491(v) values(val) returning k into new_k;
        select v into new_val from t491 where k = new_k;
        checksum = checksum + new_val;
      when 492 then
        insert into t492(v) values(val) returning k into new_k;
        select v into new_val from t492 where k = new_k;
        checksum = checksum + new_val;
      when 493 then
        insert into t493(v) values(val) returning k into new_k;
        select v into new_val from t493 where k = new_k;
        checksum = checksum + new_val;
      when 494 then
        insert into t494(v) values(val) returning k into new_k;
        select v into new_val from t494 where k = new_k;
        checksum = checksum + new_val;
      when 495 then
        insert into t495(v) values(val) returning k into new_k;
        select v into new_val from t495 where k = new_k;
        checksum = checksum + new_val;
      when 496 then
        insert into t496(v) values(val) returning k into new_k;
        select v into new_val from t496 where k = new_k;
        checksum = checksum + new_val;
      when 497 then
        insert into t497(v) values(val) returning k into new_k;
        select v into new_val from t497 where k = new_k;
        checksum = checksum + new_val;
      when 498 then
        insert into t498(v) values(val) returning k into new_k;
        select v into new_val from t498 where k = new_k;
        checksum = checksum + new_val;
      when 499 then
        insert into t499(v) values(val) returning k into new_k;
        select v into new_val from t499 where k = new_k;
        checksum = checksum + new_val;
      when 500 then
        insert into t500(v) values(val) returning k into new_k;
        select v into new_val from t500 where k = new_k;
        checksum = checksum + new_val;
      when 501 then
        insert into t501(v) values(val) returning k into new_k;
        select v into new_val from t501 where k = new_k;
        checksum = checksum + new_val;
      when 502 then
        insert into t502(v) values(val) returning k into new_k;
        select v into new_val from t502 where k = new_k;
        checksum = checksum + new_val;
      when 503 then
        insert into t503(v) values(val) returning k into new_k;
        select v into new_val from t503 where k = new_k;
        checksum = checksum + new_val;
      when 504 then
        insert into t504(v) values(val) returning k into new_k;
        select v into new_val from t504 where k = new_k;
        checksum = checksum + new_val;
      when 505 then
        insert into t505(v) values(val) returning k into new_k;
        select v into new_val from t505 where k = new_k;
        checksum = checksum + new_val;
      when 506 then
        insert into t506(v) values(val) returning k into new_k;
        select v into new_val from t506 where k = new_k;
        checksum = checksum + new_val;
      when 507 then
        insert into t507(v) values(val) returning k into new_k;
        select v into new_val from t507 where k = new_k;
        checksum = checksum + new_val;
      when 508 then
        insert into t508(v) values(val) returning k into new_k;
        select v into new_val from t508 where k = new_k;
        checksum = checksum + new_val;
      when 509 then
        insert into t509(v) values(val) returning k into new_k;
        select v into new_val from t509 where k = new_k;
        checksum = checksum + new_val;
      when 510 then
        insert into t510(v) values(val) returning k into new_k;
        select v into new_val from t510 where k = new_k;
        checksum = checksum + new_val;
      when 511 then
        insert into t511(v) values(val) returning k into new_k;
        select v into new_val from t511 where k = new_k;
        checksum = checksum + new_val;
    end case;
    j := j + 1;
  end loop;
  return checksum;
end;
$function$;

select checksum_from_insert_rows('{1, 2, 4, 8, 16, 32}'::int[]);
