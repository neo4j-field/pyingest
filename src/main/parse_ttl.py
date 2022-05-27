import re

class TTLParser():
    class InvalidTTLDocument(Exception):
        pass

    def split_and_keep(self,seperator, s, maxsplit=0):
        TEMP_PLACEHOLDER = ';"_;"_'
        return re.split(TEMP_PLACEHOLDER, re.sub(seperator, lambda match: TEMP_PLACEHOLDER + match.group() , s),maxsplit=maxsplit)

    def parse_ttl_line(self, line, new_rec, prev_subject,prefixes):
        if len(line.strip()) == 0:
            return None

        try:
            cols = self.split_and_keep(r' (".*"|<http|<ftp)',line.strip(),maxsplit=2)
            cols = [col.strip() for col in cols]

            if cols[2][0] == '<':
                cols.append(cols[2][-1])
                cols[2] = cols[2][1:-3]
            else:
                if '^^' in cols[2]:
                    index = cols[2].index('^^')
                    cols.append(cols[2][-1])
                    cols.append(cols[2][index+2:-3])
                    cols[2] = cols[2][1:index-1]
                else:
                    cols2 = self.split_and_keep(r'@[a-z]{2} \.|@[a-z]{2} ;', cols[2])
                    cols[2] = cols2[0]
                    if len(cols2) > 1:
                        cols3 = re.split(r'@[a-z]{2} ', cols2[1])
                        cols.append(cols3[1])

            row = {}
            if new_rec:
                if len(cols) != 4 and len(cols) != 5:
                    raise self.InvalidTTLDocument("Error! Invalid TTL Document!")

                subjectPrefix = [prefix for prefix in prefixes if prefix['alias'] in cols[0]]
                if len(subjectPrefix) == 1:
                    row['subject'] = cols[0].replace(subjectPrefix[0]['alias'],subjectPrefix[0]['prefix'])
                else:
                    row['subject'] = self.check_first_and_last_char(cols[0])

                predicatePrefix = [prefix for prefix in prefixes if prefix['alias'] in cols[1]]
                if len(predicatePrefix) == 1:
                    row['predicate'] = cols[1].replace(predicatePrefix[0]['alias'],predicatePrefix[0]['prefix'])
                else:
                    row['predicate'] = self.check_first_and_last_char(cols[1])

                objectPrefix = [prefix for prefix in prefixes if prefix['alias'] in cols[2]]
                if len(objectPrefix) == 1:
                    row['object'] = cols[2].replace(objectPrefix[0]['alias'],objectPrefix[0]['prefix'])
                else:
                    row['object'] = self.check_first_and_last_char(cols[2])

                if len(cols) == 5:
                    row['type'] = self.check_first_and_last_char(cols[4])

                if cols[3] == ';':
                    new_rec = False
                    prev_subject = row['subject']
                elif cols[3] == '.':
                    new_rec = True
                else:
                    raise self.InvalidTTLDocument("Error! Invalid TTL Document!")
            else:
                if len(cols) != 3 and len(cols) != 4:
                    raise self.InvalidTTLDocument("Error! Invalid TTL Document!")

                row['subject'] = prev_subject

                predicatePrefix = [prefix for prefix in prefixes if prefix['alias'] in cols[0]]
                if len(predicatePrefix) == 1:
                    row['predicate'] = cols[0].replace(predicatePrefix[0]['alias'], predicatePrefix[0]['prefix'])
                else:
                    row['predicate'] = self.check_first_and_last_char(cols[0])

                objectPrefix = [prefix for prefix in prefixes if prefix['alias'] in cols[1]]
                if len(objectPrefix) == 1:
                    row['object'] = cols[1].replace(objectPrefix[0]['alias'], objectPrefix[0]['prefix'])
                else:
                    row['object'] = self.check_first_and_last_char(cols[1])

                if len(cols) == 4:
                    row['type'] = self.check_first_and_last_char(cols[3])

                if cols[2] == ';':
                    new_rec = False
                elif cols[2] == '.':
                    new_rec = True
                    prev_subject = None
                else:
                    raise self.InvalidTTLDocument("Error! Invalid TTL Document!")

            return row, new_rec, prev_subject
        except:
            print('Line: %s' % line)
            raise

    def parse_ttl_prefix(self,line):
        cols = line.strip().split(' ')

        if cols[0] != '@prefix':
            return

        return {'alias': cols[1], 'prefix': cols[2][1:len(cols[2])-1]}

    def read_prefixes(self,file):
        prefixes = []
        self.line = file.readline()
        while len(self.line) != 0:
            if self.line == '\n':
                self.line = file.readline()
                continue

            prefix = self.parse_ttl_prefix(self.line)

            if prefix == None:
                break

            prefixes.append(prefix)

            self.line = file.readline()

        return prefixes

    def read_data(self, file, prefixes, chunk_size):
        new_rec, rows, prev_subject = True, [], None

        last_subject = None
        while len(self.line) != 0:
            if self.line == '\n':
                self.line = file.readline()
                continue

            row, new_rec, prev_subject = self.parse_ttl_line(self.line, new_rec, prev_subject, prefixes)
            if not row:
                self.line = file.readline()
                continue

            rows.append(row)

            if last_subject != row['subject'] and len(rows) >= chunk_size:
                break

            last_subject = row['subject']

            self.line = file.readline()

        return rows

    def check_first_and_last_char(self,str):
        str = str[1:] if str[0] == '<' else str
        str = str[:-1] if str[len(str) - 1] == '>' else str

        if str[0] == '"':
            str = str[1:]
            str = str[:-1]


        return str