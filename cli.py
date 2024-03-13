from typing import Optional

class GenericMenu:
    def __init__(self, title: str="Generic Menu"):
        self.title=title
        self.__options: list[dict] = []
        self.__info: list = []

    def add_option(self, name: str, command: Callable, description: str = "") -> None:
        if name in self.__options:
            raise ValueError(f"Option \"{name}\" is already in the option menu.")
        else:
            self.__options.append({"name": name, "command:": command, "description": description})

    def add_info(info: str):
        self.__info.append(info)
    
    def display(self) -> None:
        
        print("--------", self.title, "--------")
        
        if self.__info:
            for line in self.__info:
                print(line)
                
        if self.__info and self.__options:
            print("\n\n")
            
        if self.__options:
            for i in range(len(self.__options)):
                print(f"{i + 1}) {self.__options[i]["name"]}")
                print(f"    Description: {self.__options[i]["description"]}")

    def __get_choice(self) -> int:
        if self.__options():
            choice = input("Choice: ")
            if choice.is_num():
                if int(choice) in range(len(self.__options)):
                    return int(choice)
                else:
                    print("Choice out of range, please try again.")
                    return self.__get_choice()
            else:
                print("Choice was not a number, please try again.")
                return self.__get_choice()
        else:
            raise ValueError("There are no choices to be made - no options!")

    def _call_choice(self, choice: int) -> None:
        self.__options[choice]["command"]()
    
    def display_and_call_choice(self) -> int:
        self.display()
        return self.__get_choice()


class ContactViewer(GenericMenu):
    def __init__(self, id: int, protocol_type: type, url: str, port: int):
        GenericMenu.__init__(self, title="Our contact")
        self.id = id
        self.url = url
        self.port = port
        self.protocol_type = protocol_type
        self.add_info(f"ID: {self.id}")
        self.add_info(f"Protocol type: {self.protocol_type}")
        self.add_info(f"URL/IP: {self.url}")
        self.add_info(f"Port: {self.port}")
        self.display_and_call_choice()
        
