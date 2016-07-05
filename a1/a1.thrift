namespace java a1

exception IllegalArgument {
    1: string message;
}

service KeyValueService {
    list<binary> multiGet(1: list<string> keys);
    list<binary> multiPut(1: list<string> keys, 2: list<binary> values)
                          throws (1: IllegalArgument ia);
    list<string> getGroupMembers();
}
