#!/usr/bin/python
#
# find all the unicode hyphen-like characters
# http://www.unicode.org/versions/latest/ch06.pdf
# in titles and print a report
#
# this can be used as an example for how to scan
# specific MARC fields for specific characters
#
# ThS 2/2014

from invenio.search_engine import get_all_field_values

unicodehyphens = {
'002D':'hyphen-minus',
'058A':'armenian hyphen',
'05BE':'hebrew punctuation maqaf',
'1806':'mongolian todo soft hypen',
'2010':'hyphen',
'2011':'non-breaking hyphen',
'2012':'figure dash',
'2013':'en dash',
'2014':'em dash',
'2015':'horizontal bar',
'2212':'minus sign',
'2E3A':'two-em dash',
'2E3B':'three-em dash',
'FE58':'small em dash',
'FE63':'small hyphen-minus',
'FF0D':'full width hyphen-minus',
}

def scan_field(field):
    if not field:
        return
    entries = get_all_field_values(field)
    if not entries:
        return
    containspattern = {}
    for item in entries:
        item = unicode(item, 'utf-8')
        for c in item:
            if ord(c) > 255 or ord(c) == int('002D', 16):
                for k in unicodehyphens.keys():
                    if ord(c) == int(k, 16):
                        containspattern.setdefault(k, []).append(item)
                        break
    return(containspattern)

def create_report(somehash):
    out = ''
    if not type(somehash) == dict:
        return(out)
    for k in somehash.keys():
        if k == '002D':
            out += '*'*79 + '\n'
            out += "%s entries with regular ASCII hyphen-minus.\n" \
                % (len(set(somehash.get(k))))
        else:
            out += '*'*79 + '\n'
            out += unicodehyphens.get(k) + ":\n\t" + "\n\t".join(set(somehash.get(k))) + '\n'
    return(out)


if __name__ == '__main__':
    print "\n*** scanning titles for hyphen like characters ***\n\n"
    hyphentitles = scan_field('245__a')
    x = create_report(hyphentitles)
    print(x)
