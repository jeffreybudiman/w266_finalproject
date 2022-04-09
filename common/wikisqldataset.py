class wikisql:

  def __init__(self, tokenizer, data_file, table_file, task="translate", experiment_type=0, numrows=0, augment_type="none"):
    self.tokenizer = tokenizer
    #self.tokenizer.bos_token = '<s>'
    self.tokenizer.sep_token = '<sep>'
    #self.tokenizer.eos_token = '</s>'
    self.data_file = data_file
    self.table_file = table_file
    self.task = task
    self.experiment_type = experiment_type
    self.numrows = numrows
    self.augment_type = augment_type
    self.tables = {}
    self.questions = []
    self.sql_lf = []
    self.table_id = []
    self.columns = []
    self.columns_types = []
    self.input_string = []
    self.target_string = []
    self.tokenized_inputs = []
    self.tokenized_targets = []
    self.task_prefix = {"translate": "translate English to SQL: ",
                        "classify_agg": "predict SQL aggregator: ",
                        "classify_sel": "predict SQL select column: ",
                        "classify_conds": "predict SQL where conditions: "}
    self.cond_ops = ['=', '>', '<', 'OP']
    self.agg_ops = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
    self.max_input_len = 512
    self.max_output_len = 200
    self.num_synonym = 2

    # check task validity
    if self.task_prefix.get(self.task) == None:
      sys.exit(f"invalid task '{self.task}'. Valid choices: 'translate'/'classify_agg'/'classify_sel'/'classify_conds' ")

    # build table dictionary (collection of all tables indexed by table id)
    with open(self.table_file) as f:
      lines = f.readlines()
      for line in lines:
        t = json.loads(line.strip())
        self.tables[t["id"]] = t

    # extract dataset json file
    with open(self.data_file) as f:
      lines = f.readlines()
      for line in lines:
        d = json.loads(line.strip())
        q = d['question'].lower()
        self.questions.append(q)
        s = d['sql']
        self.sql_lf.append(s)
        id = d['table_id']
        self.table_id.append(id)
        c = list(map(str, self.tables[d['table_id']]['header']))
        self.columns.append(c)
        ct = self.tables[d['table_id']]['types']
        self.columns_types.append(ct)
        r = self.tables[d['table_id']]['rows']

        # generate input and target label strings
        ins, ts = self.genInout(question=q, tableid=id, col=c, coltype=ct, sql_lf=s, rows=r)     
        self.input_string.extend(ins)
        self.target_string.extend(ts)

        # tokenize input and target label strings
        for (x, (i,t)) in enumerate(zip(ins, ts)):
          tok_ins, tok_ts = self.tokenizeInout(input_string=i, target_string=t)
          self.tokenized_inputs.append(tok_ins)
          self.tokenized_targets.append(tok_ts)

    #self.tokenized_inputs, self.tokenized_targets = self.tokenizeInout(input_string=self.input_string, target_string=self.target_string)

  def genInout(self, question, tableid, col, coltype, sql_lf, rows):

    aug = self.augment_type
    replace_col = False

    # if augmented is 'mixed', randomly select one of the two methods
    if aug=="mix":
      choice = ["column", "synonym"]
      aug = choice[np.random.randint(len(choice))]

    # set prefix according to the task
    prefix = self.task_prefix.get(self.task)

    # if augmentation is selected, process original question accordingly
    if aug=="none":
      pass
    elif aug=="column":
      # random select column replacement
      rand_col_id = np.random.randint(len(col))
      sel_col = col[sql_lf["sel"]].lower()
      new_col = col[rand_col_id]
      if question.find(sel_col) != -1:
        replace_col = True
        question = question.replace(sel_col, new_col)
    elif aug=="synonym":
      # random word synonym replacement
      n = min(self.num_synonym, len(question.split()))
      question = self.synonym_replacement(question, n)
    else:
      sys.exit(f"invalid augment_type '{self.augment_type}'. Valid choices: 'none' / 'column' / 'synonym' / 'mix' ")

    # input string
    instring = []
    if self.experiment_type == 0:
      txt = prefix + question 
    elif self.experiment_type == 1:
      txt = prefix + question + self.tokenizer.sep_token + tableid
      for c in col:
        txt += self.tokenizer.sep_token + c      
    elif self.experiment_type == 2:
      txt = prefix + question + self.tokenizer.sep_token + tableid
      for (i, (c, ct)) in enumerate(zip(col, coltype)):
        txt += self.tokenizer.sep_token + c + self.tokenizer.sep_token + ct 
    elif self.experiment_type == 3:
      if self.numrows > 0:
          nr = min(self.numrows, len(rows))
          selected_rows = rows[:nr]
      txt = prefix + question + self.tokenizer.sep_token + tableid
      for (i, (c, ct)) in enumerate(zip(col, coltype)):
        txt += self.tokenizer.sep_token + c + self.tokenizer.sep_token + ct 
        # insert table values
        if self.numrows > 0:
          for r in selected_rows:
            txt += self.tokenizer.sep_token + str(r[i])
    else:
      sys.exit("invalid experiment type.")
 
    #txt += self.tokenizer.eos_token
    txt = txt.lower()
    instring.append(txt)

    # output / target label string
    if aug=="column" and replace_col:
      selcol = new_col
    else:
      selcol = col[sql_lf['sel']]

    outstring = []
    if self.task=="translate":
      if sql_lf['agg'] > 0:
          txt = 'SELECT ' + "(" + self.agg_ops[sql_lf['agg']] + ")"
      else:
          txt = 'SELECT ' 
      txt += ' [' +  selcol + '] FROM [' + tableid +"] "
    
      if len(sql_lf['conds']) > 0:
          txt += 'WHERE '
          op_temp = ['equals to', 'less than', 'greater than', 'OP']
          for c in sql_lf['conds']:
              #txt += '[' + col[c[0]] + " " + self.cond_ops[c[1]]
              txt += '[' + col[c[0]] + " " + op_temp[c[1]]
              if isinstance(c[2], (int, float)):
                  txt += " " + str(c[2]) + ']'
              else:
                  #txt += " '" + c[2] + "']"
                  txt += " " + c[2] + "]"
              txt += " AND "
          txt = txt[:-5]           
    elif self.task == "classify_agg":
      ##agglist = ['none', 'maximum', 'minimum', 'count', 'sum', 'average']
      ##txt = agglist[sql_lf['agg']]
      txt = self.agg_ops[sql_lf['agg']]
    elif self.task == "classify_sel":
      txt = selcol
    elif self.task == "classify_conds":
      op_temp = ['equals to', 'less than', 'greater than', 'OP']
      if len(sql_lf['conds']) > 0:
        txt = ""
        for c in sql_lf['conds']:
          col_id = c[0]
          cond_col = col[col_id]
          op_id = c[1]
          #cond_op = self.cond_ops[op_id]
          cond_op = op_temp[op_id]

          cond_val = str(c[2])
          txt += "[" + cond_col + " " + cond_op + " " + cond_val + "]" 
      else:
        txt = ""
    else:
      sys.exit("invalid task. Choices: 'translate', 'classify' ")

    txt = txt.lower()
    outstring.append(txt)

    return instring, outstring

  def get_synonyms(self, word):
    """
    Get synonyms of a word
    """
    synonyms = set()
    
    for syn in wordnet.synsets(word): 
        for l in syn.lemmas(): 
            synonym = l.name().replace("_", " ").replace("-", " ").lower()
            synonym = "".join([char for char in synonym if char in ' qwertyuiopasdfghjklzxcvbnm'])
            synonyms.add(synonym) 
    
    if word in synonyms:
        synonyms.remove(word)
    
    return list(synonyms)

  def synonym_replacement(self, words, n):
    
    stop_words = list(set(stopwords.words('english')))
    words = words.split()
    
    new_words = words.copy()
    random_word_list = list(set([word for word in words if word not in stop_words]))
    random.shuffle(random_word_list)
    num_replaced = 0
    
    for random_word in random_word_list:
        synonyms = self.get_synonyms(random_word)
        
        if len(synonyms) >= 1:
            synonym = random.choice(list(synonyms))
            new_words = [synonym if word == random_word else word for word in new_words]
            num_replaced += 1
        
        if num_replaced >= n: #only replace up to n words
            break

    sentence = ' '.join(new_words)

    return sentence

  def tokenizeInout(self, input_string, target_string):
    # tokenize inputs
    tokenized_inputs = tokenizer.batch_encode_plus(
        [input_string], max_length=self.max_input_len, padding='max_length', return_tensors="pt"
    )    
    # tokenize targets
    tokenized_targets = tokenizer.batch_encode_plus(
        [target_string], max_length=self.max_output_len, padding='max_length', return_tensors="pt"
    )
    return tokenized_inputs, tokenized_targets
    

