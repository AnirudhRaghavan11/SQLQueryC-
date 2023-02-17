#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <utility>
//#include <variant>
#include <sstream>

using namespace std;


vector<string> parse_line_based_sep(string line, char sep = ','){
    stringstream strm(line);
    string data;
    vector<string> data_vec;
    while(getline(strm, data, sep)){
        data_vec.push_back(data);
    }
    return data_vec;
}

//using ColData = variant<int, float, string>;
//using ColData = string;

struct ColData{
    int int_val;
    float float_val;
    string string_val;
    char char_val;

    
};


class ColInfo{
    private: 
        string _col_name;
        int _col_id;
        string _col_type;
    public:
        ColInfo(){};

        ColInfo(string col_name, int col_id, string col_type) :
		_col_name(col_name), _col_id(col_id), _col_type(col_type)
	{
        }
	string getColName(){
        return _col_name;
    }
    int getColId(){
        return _col_id;
    }
    string getColType(){
        return _col_type;
    }

    string setColName(string col_name){
        this->_col_name = col_name;
    }

    // ColData parse_data(string data){
    //     if(_col_type == "INT"){
    //        return ColData(stoi(data));
    //     }
    //     else if(_col_type == "FLOAT"){
    //         return ColData(stof(data));
    //     }
    //     else{
    //         return ColData(data);
    //     }
    // }
    
};

class dbRow{
    private:
        vector<string> col_data_vec;

    public: 
        dbRow(){}

    void add_col_data(string col_data){
        //col_data = new ColData();
        //took out move from push_back(move(col_data))
        col_data_vec.push_back(col_data);
    }
    //Function to filter row 
    // bool filter_row(vector<WhereClause> clauses_r){
    //     bool condition_met = false;
    //     for (auto iter = clauses_r.begin(); iter != clauses_r.end(); iter++){
    //         const lhs_r = col_data_vec[iter->getColId() - 1];
    //     }
    // }
    
    vector<string> getColDataVec(){
        return this->col_data_vec;
    }
};

class WhereClause
{
public:
	WhereClause(string col_name, string condition, string rhs_data) :
            _col_name(col_name), _condition(condition), _rhs_data(rhs_data) {}

private:
    string _col_name;
    string _condition;
    string _rhs_data;
};

bool conditionMatch(string lhs_r, string rhs_r, string condition){
    if(condition == "="){
        return lhs_r == rhs_r;
    }
    else if(condition == ">"){
        return lhs_r > rhs_r;
    }
    else if(condition == "<"){
        return lhs_r < rhs_r;
    }
    else if(condition == ">="){
        return lhs_r >= rhs_r;
    }
    else if(condition == "<="){
        return lhs_r <= rhs_r;
    }
    else if(condition == "!="){
        return lhs_r != rhs_r;
    }
    else{
        return false;
    }
}

struct Table{
	string _table_name;
	vector<ColInfo> col_info_vec;
	vector<dbRow> db_data_vec;
	bool columns_sorted = false;

	Table(string tableName){
        this->_table_name = tableName;
    }

	bool load_column(vector<string> col_data_r){
        //check if col_data size is 4
		if(col_data_r.size() != 4){
            return false;
        }
	        //check if type is int, string or float
		col_info_vec.push_back(ColInfo(col_data_r[1], stoi(col_data_r[3]), col_data_r[2]));
        return true;
	}

	void load_data(string line_r){
		if(!columns_sorted){
			//sorting col_info_vec based on column id
			sort(col_info_vec.begin(), col_info_vec.end(), [](ColInfo &a, ColInfo &b){
			return a.getColId() < b.getColId(); });
        }
		columns_sorted = true;

		vector<string> data_vec = parse_line_based_sep(line_r);
		/*if(data_vec.size() != col_info_vec.size()){
			//throw error
			cout << "Error: Data size does not match column size" << endl;
        	}*/

		dbRow row;
        auto data_iter = data_vec.begin();
        for (auto iter = col_info_vec.begin(); iter != col_info_vec.end(); ++iter, ++data_iter)
        {
            row.add_col_data(*data_iter);
        }
        db_data_vec.push_back(row);
	}

    void sortColumns(vector<ColInfo> col_info_vect){
        sort(col_info_vect.begin(), col_info_vect.end(), [](ColInfo &a, ColInfo &b){
			return a.getColId() < b.getColId(); });
    }
    

    
    void printAllColumns(vector<ColInfo> col_info_vec){
         cout << "In print columns" << endl;
        for (auto iter = col_info_vec.begin(); iter != col_info_vec.end(); ++iter){
            cout << iter->getColName() << " ";
        }
    }

    void printAllRows(){
        for(auto iter = this->db_data_vec.begin(); iter != this->db_data_vec.end(); ++iter){
            for(auto val : iter->getColDataVec()){
                cout << val << ",";
            }
            cout << endl;
        }
    }
    //adding this new function here to check if I can print the exact columns needed
    void printSelectRows(int col_pos){
        for(auto iter = this->db_data_vec.begin(); iter != this->db_data_vec.end(); ++iter){
            cout << iter->getColDataVec()[col_pos] << ",";
        }
    }
    

    //error happening here too
    void printRows(vector<dbRow> db_data_vec){
        cout << "In printRows" << endl;
        cout << "Size of db_data_vec: " << db_data_vec.size() << endl;
        vector<dbRow>::iterator iter;
        for(iter = db_data_vec.begin(); iter != db_data_vec.end(); ++iter){
            //cout << "Entering first for loop" << endl;
            //cout << "Size of col_data_vec: " << iter->getColDataVec().size() << endl;
            for(auto val : iter->getColDataVec()){
                //cout << "Column Data Vector Size: " <<  iter->getColDataVec().size() << endl;
                cout << val << " ";
            }
        }
        
    }
};

vector<Table> table_vec;

class SelectCmd{
    private: 
        bool _select_parse_successful;
        string _err_str;
        Table *_table_p;
        //not using where clause for now
        vector<WhereClause> where_clauses_vec;
        vector<ColInfo> print_col_info;
    public:
	bool check_inclusive(string each_col)
	{
		vector<string> col_name_val = parse_line_based_sep(each_col, ':');
		return col_name_val[1] == "1";
	}
	vector<string> get_col_names(vector<string> col_str_vec)
	{
		vector<string> col_names;
		for_each(begin(col_str_vec), end(col_str_vec),
		[&col_names](const string& each_col) {
			col_names.push_back(parse_line_based_sep(each_col, ':')[0]);
		});
		return col_names;
	}
        void parse_col_list(string col_list_r)
        {
		vector<string> col_str_vec = parse_line_based_sep(col_list_r);
		bool flag = check_inclusive(col_str_vec[0]); 
		vector<string> col_names = get_col_names(col_str_vec);
		for(auto iter1 = begin(_table_p->col_info_vec); 
			iter1 != end(_table_p->col_info_vec); ++iter1) {
			auto iter2 = find_if(begin(col_names), end(col_names),
		[&iter1](const string& col_name) {
			return col_name == iter1->getColName();
		});
			if (flag) {
				if (iter2 != end(col_names)) {
					print_col_info.push_back(*iter1);	
				}
			} else {
				if (iter2 == end(col_names)) {
					print_col_info.push_back(*iter1);	
				}
			}
		}
	}

        SelectCmd(string input_line){

        string _err_str;    
        cout << "In SelectCmd constructor: " << input_line << endl;
        istringstream istrm(input_line);
        string cmd;
        istrm >> cmd;
        if (cmd != "SELECT")
        {
        _err_str = "Error: Invalid command";
        return;
            }
            string col_list;
            istrm >> col_list;
            cout << "col_list: " << col_list << endl;
            istrm >> cmd;
            //cout << "cmd: " << cmd << endl;
            if(cmd != "FROM"){
                //cout << "in from if statement" << endl;
                _err_str = "Error: no FROM clause";

                return;
            }
            //cout << "before table name" << endl;
            
            //_table_p = new Table("name");
            string table_name;
            //string table_name_req;
            istrm >> table_name;
            auto iter = find_if(table_vec.begin(), table_vec.end(), [table_name](Table &tbl){
            return tbl._table_name == table_name;
            });
            if (iter == table_vec.end()) {
                cout << "Invalid table name: " << table_name;
                return;
	        }
            _table_p = &(*iter);
            cout << "table_name: " <<  _table_p->_table_name << endl;
	    
	    parse_col_list(col_list);
            
        
            //check if table exists in the tables_vec and if not throw error
            string where_clauses_s;
            istrm >> cmd;
            //cout << "Printing cmd before where: " << cmd << endl;
            if(cmd == "WHERE"){
                //parse where clause and store it in the where_clauses_vec
                istrm >> where_clauses_s;
            }
            cout << "where_clauses_s: " << where_clauses_s << endl;
            
            //cout << "Printing cmd after where: " << cmd << endl;
            string orderby_s;   
            string condition_req;
            istrm >> condition_req;
            cout << "condition_req: " << condition_req << endl;
            string valCheckFor;
            istrm >> valCheckFor;
            cout << "valCheckFor: " << valCheckFor << endl;
            istrm >> cmd;
            //cout << "Printing cmd before orderby: " << cmd << endl;
            
            
            if(cmd == "ORDERBY"){
                istrm >> orderby_s;
                cout << "orderby_s: " << orderby_s << endl;
            }
            istrm >> cmd;
            // if(cmd == ";"){
            //     cout << "End of select command" << endl;
            // }

            cout << "before parse select" << endl;
            cout << "col_list: " << col_list << endl;
            processSelect(col_list, _table_p, table_name, where_clauses_s, orderby_s, condition_req, valCheckFor);
            
            
        }

        void processSelect(string col_list, Table *table_p, string table_name, string where_clauses_s, string orderby_s, string condition_req, string valCheckFor){
            if(col_list == "*"){
                //print all columns
                cout << "Printing all columns" << endl;
                _table_p->printAllRows();
                return;
            }
            
                //parse select clause
                
                //first we parse the select clause by colons and store it in a vector
                //size_t pos = 0;
            string token;
            //cout << "col_list: " << col_list << endl;
            
            vector<string> cols;
            vector<string> found_cols;
            stringstream ss(col_list);
            string temp;
            vector<string> col_list_vec;
            vector<string> col_list_vec_zero;
            size_t comma_pos = col_list.find(",");
            while(comma_pos != string::npos){
                temp = col_list.substr(0, comma_pos);
                size_t colon_pos = temp.find(":");
                string val_after = temp.substr(colon_pos + 1);
                //Add the column name that appears before the colon to the vector if it is followed by a "1"
                if(val_after == "0"){
                    //cout << "is val after 0:" << val_after << endl;
                    string val_zero_before = temp.substr(0, colon_pos);
                    col_list_vec_zero.push_back(val_zero_before);
                }
                if(val_after == "1"){
                    //cout << "is val after 1: " << val_after << endl;
                    string val_before = temp.substr(0, colon_pos);
                    col_list_vec.push_back(val_before);
                }
                
                //remove the substring and the comma from the original string
                col_list.erase(0, comma_pos + 1);
                //look for the next comma
                comma_pos = col_list.find(",");
            }
            //add the last column to the vector
            string last_col = col_list;
            size_t last_colon_pos = last_col.find(":");
            string val_after = last_col.substr(last_colon_pos + 1);

            if(val_after == "0"){
                //cout << "is val after 0: " << val_after << endl;
                string val_zero_before = last_col.substr(0, last_colon_pos);
                col_list_vec_zero.push_back(val_zero_before);
            }

            if(val_after == "1"){
                //cout << "is val after 1: " << val_after << endl;
                string val_before = last_col.substr(0, last_colon_pos);
                col_list_vec.push_back(val_before);
            }
            
            for(auto iter = col_list_vec.begin(); iter != col_list_vec.end(); ++iter){
                //cout << "1 Column: " << *iter << endl;
            }
            for(auto iter = col_list_vec_zero.begin(); iter != col_list_vec_zero.end(); ++iter){
                //cout << "0 Column: " << *iter << endl;
            }
            cout << "size off col_list_vec_zero: " << col_list_vec_zero.size() << endl;
            cout << "Size of col_list_vec: " << col_list_vec.size() << endl;
            //this line fixed the problem that print_col_info did not have anything in it. Don't know how it will react with other stuff, gotta check
            this->print_col_info = _table_p->col_info_vec;
            //cout << "Size of print_col_info: " << this->print_col_info.size() << endl;
            for (auto iter7 = col_list_vec.begin(); iter7 != col_list_vec.end(); ++iter7){
                //cout << " Column Req: " << *iter7 << endl;
            }
                // then we can loop through the vector of columns and check if they exist in the table
                // vector<ColInfo> check_for_column;
                // cout << "Before the first for loop to check if columns exist" << endl;
                // now it is not entering both the for loops, and nothing is getting printed
                vector<ColInfo>::iterator iter2;
            for (iter2 = this->print_col_info.begin(); iter2 != this->print_col_info.end(); ++iter2){
                for(auto iter3 = col_list_vec.begin(); iter3 != col_list_vec.end(); ++iter3){
                    if(iter2->getColName() == *iter3){
                        //cout << "Column: " << iter2->getColName() << " exists" << endl;
                        found_cols.push_back(iter2->getColName());
                    }
                }
            }
            int col_count = 0;
            // for (auto find_count = found_cols.begin(); find_count != found_cols.end(); ++find_count)
            // {
            //     col_count = countAtWhichColumn(*find_count);
            //     //_table_p->printSelectRows(col_count);
            //     cout << "Column: " << *find_count << " is at column id: " << col_count << endl;
            //     cout << "Column: " << *find_count << " is at column id: " << col_count << endl;
            //     cout << "Column: " << *find_count << " is at column id: " << col_count << endl;

            // }
            //keep this code, can use it for select * with where
            

            for (int i = 0; i < found_cols.size(); i++){
                for (auto iter5 = _table_p->db_data_vec.begin(); iter5 != _table_p->db_data_vec.end(); ++iter5){
                    //cout << iter5->getColDataVec()[countAtWhichColumn(found_cols[i])] << ",";
                    continue;
                    for (int j = 1; j < found_cols.size(); j++){
                        //cout << iter5->getColDataVec()[countAtWhichColumn(found_cols[i+j])] << ",";
                        break;
                    }
                }
                cout << endl;
            }

            //cout << "Checking if ProcessWhere function works: " << endl;
            //cout << endl;

            //processWhereAll(where_clauses_s, condition_req, valCheckFor);
            //cout << "Checking if ProcessWhereSpecific function works, at line 425: " << endl;
            //cout << endl;
            processWhereSpecific(where_clauses_s, condition_req, valCheckFor, found_cols);

            // vector<string> where_clauses_v = splitWhere(where_clauses_s);
            // for(auto iter = where_clauses_v.begin(); iter != where_clauses_v.end(); ++iter){
            //     cout << "Where Clause: " << *iter << endl;
            // }
            
        
           

            // vector<ColInfo> temp = getPrintColInfo();
            // writing this code to check if I can get the colid() of the columns that I want to print
            //  for (auto colID_iter = found_cols.begin(); colID_iter != found_cols.end(); ++colID_iter){
            //      auto get_col_id_iter = find_if(_table_p->col_info_vec.begin(), _table_p->col_info_vec.end(), [colID_iter](ColInfo &col_info){
            //          return col_info.getColId() == *colID_iter;
            //      });
            //      cout << "ColID: " << get_col_id_iter->getColId() << endl;
            //  }
        }

        int countAtWhichColumn(string col_name){
            int count = 0;
            vector<ColInfo> col_search_vec;
            col_search_vec = _table_p->col_info_vec;
            //_table_p->sortColumns(col_search_vec);

            //test to check if sorted 
            // for(auto test = col_search_vec.begin(); test != col_search_vec.end(); ++test){
            //     cout << "Column name col search_vec: " << test->getColName() << endl;
            // }

            for(auto iter = col_search_vec.begin(); iter != col_search_vec.end(); ++iter){
                if(iter->getColName() == col_name){
                    return count;
                }
                count++;
            }
            return -1;
        }

        
        void processWhereAll(string where_clauses_s, string condition_req, string valCheckFor){
            vector<vector<string>> where_col_val2 = getColValforWhereAll(where_clauses_s, condition_req, valCheckFor);
            for(int i = 0; i < where_col_val2.size(); i++){
                for(int j = 0; j < where_col_val2[i].size(); j++){
                    //cout << where_col_val2[i][j] << ",";
                }
                cout << endl;
            }
        }


        vector<vector<string>> getColValforWhereAll(string where_clauses_s, string condition, string what_to_compare){
            int wherePos = countAtWhichColumn(where_clauses_s);
            vector<dbRow> db_data_vec_w = _table_p->db_data_vec;
            vector<ColInfo> col_info_vec_w = _table_p->col_info_vec;
            vector<vector<string>> satisfy_where_vec;
            for (int i = 0; i < db_data_vec_w.size(); i++){
                if (condition == "="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] == what_to_compare){
                        satisfy_where_vec.push_back(db_data_vec_w[i].getColDataVec());
                    }
                }
                else if (condition == ">"){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] > what_to_compare){
                        satisfy_where_vec.push_back(db_data_vec_w[i].getColDataVec());
                    }
                }
                else if (condition == "<"){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] < what_to_compare){
                        satisfy_where_vec.push_back(db_data_vec_w[i].getColDataVec());
                    }
                }
                else if (condition == ">="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] >= what_to_compare){
                        satisfy_where_vec.push_back(db_data_vec_w[i].getColDataVec());
                    }
                }
                else if (condition == "<="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] <= what_to_compare){
                        satisfy_where_vec.push_back(db_data_vec_w[i].getColDataVec());
                    }
                }
                else if (condition == "!="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] != what_to_compare){
                        satisfy_where_vec.push_back(db_data_vec_w[i].getColDataVec());
                    }
                }
            }
                
            return satisfy_where_vec;
        }

        void processWhereSpecific(string where_clauses_s, string condition_req, string valCheckFor, vector<string> found_cols){
            vector<string> where_col_val2 = getColValforWhereSpecific(where_clauses_s, condition_req, valCheckFor, found_cols);
            int checkend;
            //cout << where_col_val2.size() << endl;
            for(int i = 0; i < where_col_val2.size(); i++){
                cout << where_col_val2[i];
                if (i % found_cols.size() == found_cols.size() - 1) {
                    cout << endl;
                } else {
                    cout << ",";
                }
            }
            // for(int i = 0; i < where_col_val2.size(); i++){
            //     cout << where_col_val2[i] << ",";
            // }
        }

        vector<string> getColValforWhereSpecific(string where_clauses_s, string condition, string what_to_compare, vector<string> found_cols){
            int wherePos = countAtWhichColumn(where_clauses_s);
            vector<dbRow> db_data_vec_w = _table_p->db_data_vec;
            vector<ColInfo> col_info_vec_w = _table_p->col_info_vec;
            vector<string> cols_with_one_vec;
            string cols_with_one;
            vector<vector<string>> satisfy_where_vec;
            for (int i = 0; i < db_data_vec_w.size(); i++){
                if (condition == "="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] == what_to_compare){
                        for (int j = 0; j < found_cols.size(); j++){
                            cols_with_one = db_data_vec_w[i].getColDataVec()[countAtWhichColumn(found_cols[j])];
                           cols_with_one_vec.push_back(cols_with_one); 
                       }
                    }
                }
                else if (condition == ">"){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] > what_to_compare){
                        for (int j = 0; j < found_cols.size(); j++){
                            cols_with_one = db_data_vec_w[i].getColDataVec()[countAtWhichColumn(found_cols[j])];
                           cols_with_one_vec.push_back(cols_with_one); 
                       }
                    }
                }
                else if (condition == "<"){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] < what_to_compare){
                        for (int j = 0; j < found_cols.size(); j++){
                            cols_with_one = db_data_vec_w[i].getColDataVec()[countAtWhichColumn(found_cols[j])];
                           cols_with_one_vec.push_back(cols_with_one); 
                       }
                    }
                }
                else if (condition == ">="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] >= what_to_compare){
                        for (int j = 0; j < found_cols.size(); j++){
                            cols_with_one = db_data_vec_w[i].getColDataVec()[countAtWhichColumn(found_cols[j])];
                           cols_with_one_vec.push_back(cols_with_one); 
                       }
                       
                    }
                    
                }
                else if (condition == "<="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] <= what_to_compare){
                        for (int j = 0; j < found_cols.size(); j++){
                            cols_with_one = db_data_vec_w[i].getColDataVec()[countAtWhichColumn(found_cols[j])];
                           cols_with_one_vec.push_back(cols_with_one); 
                       }
                    }
                }
                else if (condition == "!="){
                    if (db_data_vec_w[i].getColDataVec()[wherePos] != what_to_compare){
                        for (int j = 0; j < found_cols.size(); j++){
                            cols_with_one = db_data_vec_w[i].getColDataVec()[countAtWhichColumn(found_cols[j])];
                           cols_with_one_vec.push_back(cols_with_one); 
                       }
                    }
                }
            }

            return cols_with_one_vec;
        }

        vector<string> orderByAscending(string order_by_clause){
            size_t colon_pos = order_by_clause.find(":");

        }

            // void parseOrderBy(string order_by_clause){

            //     string order_by_col = order_by_vec[0];
            //     string order_by_order = order_by_vec[1];
            //     int order_by_pos = countAtWhichColumn(order_by_col);
            //     vector<dbRow> db_data_vec_o = _table_p->db_data_vec;
            //     vector<ColInfo> col_info_vec_o = _table_p->col_info_vec;
            //     vector<string> order_by_col_vec;
            //     string order_by_col_str;
            //     for (int i = 0; i < db_data_vec_o.size(); i++){
            //         order_by_col_str = db_data_vec_o[i].getColDataVec()[order_by_pos];
            //         order_by_col_vec.push_back(order_by_col_str);
            //     }
            //     if (order_by_order == "ASC"){
            //         sort(order_by_col_vec.begin(), order_by_col_vec.end());
            //     }
            //     else if (order_by_order == "DESC"){
            //         sort(order_by_col_vec.begin(), order_by_col_vec.end(), greater<string>());
            //     }
            //     for (int i = 0; i < order_by_col_vec.size(); i++){
            //         cout << order_by_col_vec[i] << ",";
            //     }
            // }

            bool parse_successful()
        {
            return _select_parse_successful;
        }

        // //change from putting 
        vector<string> parse_select(string col_list, vector<ColInfo> col_info_vec){

            cout << endl;
        }
};

int main(int argc, char *argv[]){
    //keep columns as a vector only
    //then you can just check the vector

    ifstream hdl("TAB_COLUMNS.CSV");
    if(!hdl.is_open()){
        cout << "Error: Unable to open tab_columns.csv" << endl;
        return 1;
    }
    cout << "Reading tab_columns.csv" << endl;
    string line;
    while(getline(hdl, line)){
        vector<string> tbl_col_vec = parse_line_based_sep(line);
        string table_name = tbl_col_vec[0];
        //to check if table already exists in table_vec
        auto iter = find_if(table_vec.begin(), table_vec.end(), [table_name](Table &tbl){
            return tbl._table_name == table_name;
        });
        if(iter == table_vec.end()){
            //table does not exist
            Table tbl(table_name);
            table_vec.push_back(move(tbl));
            iter = end(table_vec) - 1;
        }
        iter->load_column(tbl_col_vec);
    }
    hdl.close();
    //load data from the data files and check if you can print the data

    //now we load the table file
    ifstream hdl1("EMPLOYEE.CSV");
    if(!hdl1.is_open()){
        cout << "Error: Unable to open employee.csv" << endl;
        return 1;
    }
    string table_name = "EMPLOYEE";
    auto table_iter = find_if(table_vec.begin(), table_vec.end(), [table_name](Table &tbl){
            return tbl._table_name == table_name;
    });
    if(table_iter == table_vec.end()){
	cout << "Table not found" << endl;
    }
    cout << "Reading employee.csv" << endl;
    while(getline(hdl1, line)){
        table_iter->load_data(line);
    }

    hdl1.close();

    Table *table_p;
    

    string input_line;
    while(getline(cin, input_line)){
        input_line.erase(input_line.find_last_of(";"));
        SelectCmd select_cmd(input_line);
    }

    return 0;
}
