#!/usr/bin/python
# -*- coding: utf-8 -*-
import formula_builder
import datetime
import expression_parser
import common.tools


class GroupParamError(Exception):
    pass


class Info(object):
    def __init__(self, fid, info, locale_code='zh'):
        self.locale = locale_code
        self.info = info
        self.fid = fid

    def get_groups(self):
        return self.info.get('groups', [])

    def parse_group(self, group):
        return ""

    def get_default(self):
        default_name = self.info.get('default') or self.info.get('default_name')
        return default_name if default_name else '未分组' if self.locale == "zh" else "Ungrouped"

    def to_case_when(self):
        ranges = self.get_groups()
        groups = []
        for g in ranges:
            when_then = self.parse_group(g)
            if when_then:
                groups.append(when_then)
        if not groups:
            raise GroupParamError("There should be at least one group")
        return "CASE %s ELSE '%s' END" % (" ".join(groups), self.get_default())


class NumberInfo(Info):
    def parse_group(self, group):
        if group['range'][0] is not None and group['range'][1] is not None:
            return "WHEN (%s%s%s AND %s%s%s) THEN '%s'" % (
                group['range'][0],
                self.less_than(group['boundary'][0]),
                self.fid,
                self.fid,
                self.less_than(group['boundary'][1]),
                group['range'][1],
                group['name']
            )
        elif group['range'][0] is None and group['range'][1] is not None:
            return "WHEN %s%s%s THEN '%s'" % (
                self.fid,
                self.less_than(group['boundary'][1]),
                group['range'][1],
                group['name']
            )
        elif group['range'][1] is None and group['range'][0] is not None:
            return "WHEN %s%s%s THEN '%s'" % (
                self.fid,
                self.greater_than(group['boundary'][0]),
                group['range'][0],
                group['name']
            )
        else:
            return ""

    def less_than(self, b):
        if b == 0:
            return "<"
        else:
            return "<="

    def greater_than(self, b):
        if b == 0:
            return ">"
        else:
            return ">="


class StringConditionInfo(Info):
    def __init__(self, fid, info):
        Info.__init__(self, fid, info)
        self.operators = ["%s='%s'", "INSTRING(%s,'%s')!=0", "INSTRING(%s,'%s')=0"]

    def parse_group(self, group):
        conditions = []
        for cond in group['conditions']:
            conditions.append(self.operators[int(cond.get('operator', 0))] % (self.fid, cond['value']))
        return "WHEN (%s) THEN '%s'" % ((" %s " % group.get("logic", "OR")).join(conditions), group['name'])


class StringItemInfo(Info):
    def parse_group(self, group):
        items = group.get('conditions', [])
        if items:
            escaped_items = map(lambda item: common.tools.escape(item), items)
            return "WHEN (ARRAY_CONTAINS(ARRAY('%s'), %s)) THEN '%s'" % (
                "', '".join(escaped_items),
                self.fid, group['name']
            )
        else:
            raise GroupParamError("No item found")


class DateInfo(Info):
    def parse_group(self, group):
        format_date = lambda date:datetime.datetime.strftime(datetime.datetime.strptime(date, "%Y-%m-%d"), '%Y%m%d')
        get_format_key = lambda fid : "CONCAT(YEAR(%s),IF(MONTH(%s)>9,MONTH(%s),CONCAT('0',MONTH(%s))),IF(DAY(%s)>9,DAY(%s),CONCAT('0',DAY(%s))))"%((fid,)*7)
        if group['range'][0] is not None and group['range'][1] is not None:
            return "WHEN ('%s'<=%s AND %s<='%s') THEN '%s'" % (
                format_date(group['range'][0]),
                get_format_key(self.fid),
                get_format_key(self.fid),
                format_date(group['range'][1]),
                group['name']
            )
        elif group['range'][0] is None and group['range'][1] is not None:
            return "WHEN %s<='%s' THEN '%s'" % (
                get_format_key(self.fid),
                format_date(group['range'][1]),
                group['name']
            )
        elif group['range'][1] is None and group['range'][0] is not None:
            return "WHEN %s>='%s' THEN '%s'" % (
                get_format_key(self.fid),
                format_date(group['range'][0]),
                group['name']
            )
        else:
            raise GroupParamError("Invalid date range")


class DateGroupYearInfo(Info):
    def parse_group(self, group):
        get_format_key = lambda fid : "CONCAT(IF(MONTH(%s)>9,MONTH(%s),CONCAT('0',MONTH(%s))),IF(DAY(%s)>9,DAY(%s),CONCAT('0',DAY(%s))))"%((fid,)*6)
        if group['range'][0] is not None and group['range'][1] is not None:
            return "WHEN ('%s'<=%s AND %s<='%s') THEN '%s'" % (
                group['range'][0],
                get_format_key(self.fid),
                get_format_key(self.fid),
                group['range'][1],
                group['name']
            )


class DateGroupMonthInfo(Info):
    def parse_group(self, group):
        get_format_key = lambda fid : "CONCAT(IF(DAY(%s)>9,DAY(%s),CONCAT('0',DAY(%s))))"%((fid,)*3)
        if group['range'][0] is not None and group['range'][1] is not None:
            return "WHEN ('%s'<=%s AND %s<='%s') THEN '%s'" % (
                group['range'][0] if group['range'][0]>9 else "0"+str(group['range'][0]),
                get_format_key(self.fid),
                get_format_key(self.fid),
                group['range'][1] if group['range'][1]>9 else "0"+str(group['range'][1]),
                group['name']
            )


class DateGroupWeekInfo(Info):
    def parse_group(self, group):
        get_format_key = lambda fid : "DAY_OF_WEEK(%s)"%((fid,)*1)
        if group['range'][0] is not None and group['range'][1] is not None:
            return "WHEN (%s<=%s AND %s<=%s) THEN '%s'" % (
                group['range'][0],
                get_format_key(self.fid),
                get_format_key(self.fid),
                group['range'][1],
                group['name']
            )


class FixedGroupInfo(Info):
    @property
    def start(self):
        raise NotImplementedError("Stub")

    @property
    def step(self):
        return int(self.info['step'])


class NumberFixedGroupInfo(FixedGroupInfo):
    @property
    def start(self):
        return self.info['range'][0]

    @property
    def end(self):
        return self.info['range'][1]

    def to_case_when(self):
        return 'FIXED_NUMBER_GROUP(%(fid)s, %(start)s, %(end)s, %(step)s, "%(locale)s")' % {
            "fid": self.fid,
            "start": self.start,
            "end": self.end,
            "step": self.step,
            "locale": self.locale
        }


class DateFixedGroupInfo(FixedGroupInfo):
    @property
    def start(self):
        return ",".join(self.info['start'])

    @property
    def aggregator(self):
        return self.info['aggregator']

    def to_case_when(self):
        return 'FIXED_DATE_GROUP(%(fid)s, "%(start)s", %(step)s, "%(aggregator)s", "%(locale)s")' % {
            "fid": self.fid,
            "start": self.start,
            "step": self.step,
            "locale": self.locale,
            "aggregator": self.aggregator
        }


class DateGroupFixedRealTimeInfo(Info):
    def get_groups(self):
        return [self.info, ]

    def to_case_when(self):
        return Info.to_case_when(self)

    def get_default(self):
        return '未分组' if self.locale == "zh" else "Ungrouped"

    def parse_group(self, group):
        if group.get("aggregator") == 'year':
            return self.get_year_whenthen(group)
        elif group.get("aggregator") == 'quarter':
            return self.get_quarter_whenthen(group)
        elif group.get("aggregator") == 'month':
            return self.get_month_whenthen(group)
        elif group.get("aggregator") == 'week':
            return self.get_week_whenthen(group)
        elif group.get("aggregator") == 'day':
            return self.get_day_whenthen(group)

    def get_day_whenthen(self, group):
        day_add = lambda date,days :datetime.datetime.strftime(datetime.datetime.strptime(date, "%Y%m%d")+datetime.timedelta(days), '%Y%m%d')
        get_format_key = lambda fid: "CONCAT(YEAR(%s),IF(MONTH(%s)>9,MONTH(%s),CONCAT('0',MONTH(%s))),IF(DAY(%s)>9,DAY(%s),CONCAT('0',DAY(%s))))"%((self.fid,)*7)
        when_then_list = []
        flag = datetime.datetime.strftime(datetime.datetime.strptime(group['start'][0], "%Y-%m-%d"), '%Y%m%d')
        end = datetime.datetime.strftime(datetime.datetime.strptime(group['end'][0], "%Y-%m-%d"), '%Y%m%d')

        while day_add(flag,group.get('step'))<=end:
            when_then = "WHEN ('%s'<=%s AND %s<'%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(day_add(flag,group.get('step'))),
                '%s-%s'%(flag,day_add(flag,group.get('step')-1)) \
                if flag!=day_add(flag,group.get('step')-1) else '%s'%flag
                )
            flag = day_add(flag,group.get('step'))
            when_then_list.append(when_then)
        if flag<=end:
            when_then = "WHEN ('%s'<=%s AND %s<='%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(end),
                '%s-%s'%(flag,end) \
                if flag!=end else '%s'%end
                )
            when_then_list.append(when_then)
        return " ".join(when_then_list)

    def get_week_whenthen(self, group):
        week_add = lambda x,z :[x[0]+(x[1]+z-1)/53,(x[1]+z-(x[1]+z-1)/53*53)]
        get_format_key = lambda fid: "CONCAT(SUBSTR(WEEK(%s),0,4),IF(WEEK(%s,1)>9,WEEK(%s,1),CONCAT('0',WEEK(%s,1))))"%((self.fid,)*4)
        when_then_list = []
        flag = [int(group.get('start')[0]),int(group.get('start')[1])]
        end = [int(group.get('end')[0]),int(group.get('end')[1])]

        name = lambda x,y:'%s年第%s周-%s年第%s周'%(x[0],x[1],y[0],y[1]) \
                if self.locale=='zh' else 'Week %s-%s Week %s-%s'%(x[1],x[0],y[1],y[0])
        name_one = lambda x:'%s年第%s周'%(x[0],x[1]) if self.locale=='zh' else 'Week %s-%s'%(x[1],x[0])

        while self.mult_compare(week_add(flag,group.get('step')),end)!=1:
            when_then = "WHEN ('%s'<=%s AND %s<'%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(week_add(flag,group.get('step'))),
                name(flag,week_add(flag,group.get('step')-1)) \
                if self.mult_compare(flag,week_add(flag,group.get('step')-1))!=0 else name_one(flag)
                )
            flag = week_add(flag, group.get('step'))
            when_then_list.append(when_then)
        if self.mult_compare(flag, end)!=1:
            when_then = "WHEN ('%s'<=%s AND %s<='%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(end),
                name(flag, end) \
                if self.mult_compare(flag,end)!=0 else name_one(end)
                )
            when_then_list.append(when_then)
        return " ".join(when_then_list)

    def get_month_whenthen(self,group):
        month_add = lambda x,z :[x[0]+(x[1]+z-1)/12,(x[1]+z-(x[1]+z-1)/12*12)]
        get_format_key = lambda fid: "CONCAT(YEAR(%s),IF(MONTH(%s)>9,MONTH(%s),CONCAT('0',MONTH(%s))))"%((self.fid,)*4)
        when_then_list = []
        flag = [int(group.get('start')[0]),int(group.get('start')[1])]
        end = [int(group.get('end')[0]),int(group.get('end')[1])]

        name = lambda x,y:'%s年%s月-%s年%s月'%(x[0],x[1],y[0],y[1]) \
                if self.locale=='zh' else '%s-%s %s-%s'%(x[1],x[0],y[1],y[0])
        name_one = lambda x:'%s年%s月'%(x[0], x[1]) if self.locale=='zh' else '%s-%s'%(x[0],x[1])

        while self.mult_compare(month_add(flag,group.get('step')),end)!=1:
            when_then = "WHEN ('%s'<=%s AND %s<'%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(month_add(flag,group.get('step'))),
                name(flag, month_add(flag,group.get('step')-1)) \
                if self.mult_compare(flag,month_add(flag,group.get('step')-1))!=0 else name_one(flag)
                )
            flag = month_add(flag,group.get('step'))
            when_then_list.append(when_then)
        if self.mult_compare(flag,end)!=1:
            when_then = "WHEN ('%s'<=%s AND %s<='%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(end),
                name(flag, end) \
                if self.mult_compare(flag,end)!=0 else name_one(end)
                )
            when_then_list.append(when_then)
        return " ".join(when_then_list)

    def get_quarter_whenthen(self,group):
        quarter_add = lambda x,z: [x[0]+(x[1]+z-1)/4,(x[1]+z-(x[1]+z-1)/4*4)]
        get_format_key = lambda fid: "CONCAT(YEAR(%s),IF(QUARTER(%s)>9,QUARTER(%s),CONCAT('0',QUARTER(%s))))"%((self.fid,)*4)
        when_then_list = []
        flag = [int(group.get('start')[0]),int(group.get('start')[1])]
        end = [int(group.get('end')[0]),int(group.get('end')[1])]

        name = lambda x,y: '%s年%s季度-%s年%s季度'%(x[0], x[1], y[0], y[1]) \
                if self.locale=='zh' else 'Q%s-%s Q%s-%s'%(x[1], x[0], y[1], y[0])
        name_one = lambda x: '%s年%s季度'%(x[0], x[1]) if self.locale=='zh' else 'Q%s-%s'%(x[1], x[0])

        while self.mult_compare(quarter_add(flag,group.get('step')),end)!=1:
            when_then = "WHEN ('%s'<=%s AND %s<'%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(quarter_add(flag,group.get('step'))),
                name(flag, quarter_add(flag,group.get('step')-1)) \
                if self.mult_compare(flag,quarter_add(flag,group.get('step')-1))!=0 else name_one(flag)
                )
            flag = quarter_add(flag,group.get('step'))
            when_then_list.append(when_then)
        if self.mult_compare(flag,end)!=1:
            when_then = "WHEN ('%s'<=%s AND %s<='%s') THEN '%s' "%(
                self.mult_to_str(flag),
                get_format_key(self.fid),
                get_format_key(self.fid),
                self.mult_to_str(end),
                name(flag, end) \
                if self.mult_compare(flag,end)!=0 else name_one(end)
                )
            when_then_list.append(when_then)
        return " ".join(when_then_list)

    def get_year_whenthen(self, group):
        when_then_list = []
        flag = int(group.get('start')[0])
        end = int(group.get('end')[0])

        name = '%s-%s年' if self.locale=='zh' else '%s-%s'
        name_one = '%s年' if self.locale=='zh' else '%s'
        while (flag+group.get('step'))<=end:
            when_then = "WHEN (%s<=%s AND %s<%s) THEN '%s' "%(
                flag,
                'YEAR(%s)'%(self.fid),
                'YEAR(%s)'%(self.fid),
                flag+group.get('step'),
                name%(flag,(flag+group.get('step')-1)) if flag!=(flag+group.get('step')-1) else name_one%flag
                )
            flag+=group.get('step')
            when_then_list.append(when_then)
        if flag<=end:
            when_then = "WHEN (%s<=%s AND %s<=%s) THEN '%s' "%(
                flag,
                'YEAR(%s)'%(self.fid),
                'YEAR(%s)'%(self.fid),
                end,
                name%(flag,end) if flag!=end else name_one%end
                )
            when_then_list.append(when_then)
        return " ".join(when_then_list)

    def mult_to_str(self,x):
        mult_str = ""
        for ele in x:
            if ele<10:
                mult_str+=('0'+str(ele))
            else:
                mult_str+=str(ele)
        return mult_str

    def mult_compare(self,x,y):
        if x[0] == y[0] and x[1] == y[1]:
            return 0
        elif x[0] >y[0] or (x[0] == y[0] and x[1]>y[1]):
            return 1
        else:
            return -1


class ExpressionInfo(Info):
    def parse_group(self, group):
        expression = str(group.get('expression', ''))
        if expression != "":
            return "WHEN %s THEN '%s'" % (
                expression_parser.ExpressionParser.expression_replace(expression, self.fid),
                group['name']
            )
        raise GroupParamError("Invalid expression")


class GroupFormulaBuilder(formula_builder.FormulaBuilder):
    def __init__(self, param, locale_code='zh'):
        formula_builder.FormulaBuilder.__init__(self, param)
        info = param.get('info', {})
        self.type = 'group'
        if info.get('type') == 'expression':
            self.info = ExpressionInfo(self.fid, info)
        elif self.data_type == 'number':
            if info.get('type', '') == 'custom':
                self.info = NumberInfo(self.fid, info)
            elif info.get('type', '') == 'fixed':
                self.info = NumberFixedGroupInfo(self.fid, info, locale_code)
        elif self.data_type == 'string':
            if info.get('type') == 'item':
                self.info = StringItemInfo(self.fid, info)
            else:
                self.info = StringConditionInfo(self.fid, info)
        elif self.data_type == "date":
            if info.get('type', '') == 'custom':
                self.info = DateInfo(self.fid, info, locale_code)
            elif info.get('type', '') == "group_year":
                self.info = DateGroupYearInfo(self.fid, info, locale_code)
            elif info.get('type', '') == "group_month":
                self.info = DateGroupMonthInfo(self.fid, info, locale_code)
            elif info.get('type', '') == "group_week":
                self.info = DateGroupWeekInfo(self.fid, info, locale_code)
            elif info.get('type', '') == "fixed":
                self.info = DateFixedGroupInfo(self.fid, info, locale_code)
            elif info.get('type', '') == "fixed_real_time":
                # 只有老数据才会走这个分支了, 新结构不需要实时获取值再拼case when了
                self.info = DateGroupFixedRealTimeInfo(self.fid, info, locale_code)
        else:
            self.info = None

    def build(self):
        if not self.info:
            return ""
        else:
            return self.info.to_case_when()
