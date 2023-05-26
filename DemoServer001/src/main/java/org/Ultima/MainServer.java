package org.Ultima;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.TimeZone;

/***
 * Main Starter class for Server version Demo_0.0.1
 ***/
public class MainServer {

    public static String Instruction = "It's instruction for using Server version Demo_0.0.1 Community Edition\nTo start you will write server's information\nIf you successfully started you can:\n1. Enter 'e' and press enter to turn off server immediately\n2. Enter 'p' and press enter to prepare to turning off\n3. Enter 'o' and press enter to get count of online users\nWish you good use, thanks for reading instruction\n";
    public static volatile Properties ServerConfigs = null;
    public static volatile int ServerTimeOut = 30;
    public static volatile Connection ClientsDB = null;
    public static volatile Connection GroupsDB = null;
    public static volatile DateFormat DateForm = null;
    private static Scanner ConsoleIn = null;
    public static volatile ServerSocket Server = null;
    public static volatile boolean isServerWorking = true;
    private static volatile long Online_Users_Count = 0;

    public static synchronized void addClientThread() {
        Online_Users_Count++;
    }

    public static synchronized void removeClientThread() {
        Online_Users_Count--;
    }

    private static void CreateInstruction() {
        System.out.println(Instruction);
        try {
            if (!new File("Instructions/Instruction.txt").exists()) {
                if (!new File("Instructions").mkdirs()) {
                    throw new IOException();
                }
                if (!new File("Instructions/Instruction.txt").createNewFile()) {
                    throw new IOException();
                }
            }
            FileOutputStream InstructionCreator = new FileOutputStream("Instructions/Instruction.txt");
            InstructionCreator.write(ByteOperations.Get_Bytes_By_String_UTF_8(Instruction));
            InstructionCreator.close();
        } catch (Exception UnknownCreatingInstructionException) {
            System.out.println("Failed to save instruction");
        }
    }

    private static boolean LoadServerConfigs() {
        try {
            System.out.println("Starting loading configs...");
            FileInputStream ConfigsLoader = new FileInputStream("Configs/Configs.properties");
            ServerConfigs.load(ConfigsLoader);
            ConfigsLoader.close();
            ServerTimeOut = Integer.parseInt(ServerConfigs.getProperty("TimeOut", "30"));
            System.out.println("Configs successfully loaded");
            return true;
        } catch (Exception FailedToLoadException) {
            return false;
        }
    }

    private static boolean CreateServerConfigs(Scanner Input) {
        try {
            System.out.println("Creating configs...");
            System.out.print("Enter server name: ");
            String ServerName = Input.nextLine();
            System.out.println("Enter description, push one line '^^^' to end: ");
            String ServerDescription;
            StringBuilder ServerDescriptionBuilder = new StringBuilder();
            while (true) {
                String tempLine = Input.nextLine() + "\n";
                if (tempLine.equals("^^^\n")) {
                    ServerDescription = ServerDescriptionBuilder.toString();
                    if (ServerDescription.length() > 0)
                        ServerDescription = ServerDescription.substring(0, ServerDescription.length() - 1);
                    break;
                } else {
                    ServerDescriptionBuilder.append(tempLine);
                }
            }
            System.out.print("Enter is server need password ('true' need) or ('false' if not): ");
            String isPasswordNeed;
            while (true) {
                String TempValue = Input.nextLine();
                if (TempValue.equals("true") || TempValue.equals("false")) {
                    isPasswordNeed = TempValue;
                    break;
                } else {
                    System.out.print("Error, please try again: ");
                }
            }
            String ServerPassword = "";
            if (isPasswordNeed.equals("true")) {
                System.out.print("Enter password: ");
                ServerPassword = Input.nextLine();
            }
            System.out.print("Write can clients register ('true' can) or ('false' can not): ");
            String canRegister;
            while (true) {
                String TempValue = Input.nextLine();
                if (TempValue.equals("true") || TempValue.equals("false")) {
                    canRegister = TempValue;
                    break;
                } else {
                    System.out.print("Error, please try again: ");
                }
            }
            System.out.print("Write default account status(0 - admin, 1 - secured account...): ");
            long DefaultAccountStatus = Input.nextLong();
            System.out.print("Write port: ");
            int ServerPort = Input.nextInt();
            System.out.print("Write server timeout(seconds): ");
            ServerTimeOut = Input.nextInt();
            ServerConfigs.setProperty("Name", ServerName);
            ServerConfigs.setProperty("Description", ServerDescription);
            ServerConfigs.setProperty("IsPasswordNeed", isPasswordNeed);
            ServerConfigs.setProperty("CanRegister", canRegister);
            ServerConfigs.setProperty("Password", ServerPassword);
            ServerConfigs.setProperty("DefaultAccountStatus", String.valueOf(DefaultAccountStatus));
            ServerConfigs.setProperty("Port", String.valueOf(ServerPort));
            ServerConfigs.setProperty("TimeOut", String.valueOf(ServerTimeOut));
            System.out.println("Saving...");
            try {
                if (new File("Configs").mkdirs()) {
                    File CreatingFile = new File("Configs/Configs.properties");
                    if (CreatingFile.createNewFile()) {
                        FileOutputStream ConfigsSaver = new FileOutputStream(CreatingFile);
                        ServerConfigs.store(ConfigsSaver, "Configs");
                        ConfigsSaver.close();
                        System.out.println("Successfully saved");
                        return true;
                    } else {
                        System.out.println("Failed to create file");
                        return false;
                    }
                } else {
                    System.out.println("Failed to create directory");
                    return false;
                }
            } catch (Exception SavingException) {
                return false;
            }
        } catch (Exception UnknownException) {
            return false;
        }
    }

    private static boolean LoadClientsDB() {
        System.out.println("Loading clients db...");
        if (new File("Data/Clients.db").exists()) {
            try {
                ClientsDB = DriverManager.getConnection("jdbc:sqlite:Data/Clients.db");
                Statement LoadingStatement = ClientsDB.createStatement();
                LoadingStatement.execute("CREATE TABLE IF NOT EXISTS Clients(id INTEGER PRIMARY KEY AUTOINCREMENT, Email TEXT NOT NULL UNIQUE, ICN TEXT NOT NULL UNIQUE, Password TEXT NOT NULL, DataPath TEXT NOT NULL UNIQUE)");
                LoadingStatement.close();
                System.out.println("DB has been loaded");
                return true;
            } catch (Exception SQLiteException) {
                System.out.println("Unknown file or sqlite error");
            }
        }
        return false;
    }

    private static boolean CreateClientsDB() {
        System.out.println("Creating database...");
        try {
            if (new File("Data").exists()) {
                System.out.println("DBDirectory was found");
            } else {
                System.out.println("DBDirectory was not found, \nCreating DBDirectory");
                if (new File("Data").mkdirs()) {
                    System.out.println("DBDirectory was made");
                } else {
                    System.out.println("Failed to create DBDirectory, please restart");
                    return false;
                }
            }
            if (new File("Data/Clients.db").exists()) {
                System.out.println("DBFile was found");
            } else {
                System.out.println("DBFile was not found, \nCreating DBFile");
                if (new File("Data/Clients.db").createNewFile()) {
                    System.out.println("DBFile was made");
                } else {
                    System.out.println("Failed to create DBFile, please restart");
                    return false;
                }
            }
            ClientsDB = DriverManager.getConnection("jdbc:sqlite:Data/Clients.db");
            Statement CreatingStatement = ClientsDB.createStatement();
            CreatingStatement.execute("CREATE TABLE IF NOT EXISTS Clients(id INTEGER PRIMARY KEY AUTOINCREMENT, Email TEXT NOT NULL UNIQUE, ICN TEXT NOT NULL UNIQUE, Password TEXT NOT NULL, DataPath TEXT NOT NULL UNIQUE)");
            CreatingStatement.close();
            System.out.println("DB has been created");
            return true;
        } catch (Exception SQLiteException) {
            System.out.println("Unknown file or sqlite error");
            return false;
        }
    }

    private static boolean LoadGroupsDB() {
        System.out.println("Loading groups db...");
        if (new File("Data/Groups.db").exists()) {
            try {
                GroupsDB = DriverManager.getConnection("jdbc:sqlite:Data/Groups.db");
                Statement LoadingStatement = GroupsDB.createStatement();
                LoadingStatement.execute("CREATE TABLE IF NOT EXISTS Groups(id INTEGER PRIMARY KEY AUTOINCREMENT, ICN TEXT NOT NULL UNIQUE, DataPath TEXT NOT NULL UNIQUE)");
                LoadingStatement.close();
                System.out.println("DB has been loaded");
                return true;
            } catch (Exception SQLiteException) {
                System.out.println("Unknown file or sqlite error");
            }
        }
        return false;
    }

    private static boolean CreateGroupsDB() {
        System.out.println("Creating database...");
        try {
            if (new File("Data").exists()) {
                System.out.println("DBDirectory was found");
            } else {
                System.out.println("DBDirectory was not found, \nCreating DBDirectory");
                if (new File("Data").mkdirs()) {
                    System.out.println("DBDirectory was made");
                } else {
                    System.out.println("Failed to create DBDirectory, please restart");
                    return false;
                }
            }
            if (new File("Data/Groups.db").exists()) {
                System.out.println("DBFile was found");
            } else {
                System.out.println("DBFile was not found, \nCreating DBFile");
                if (new File("Data/Groups.db").createNewFile()) {
                    System.out.println("DBFile was made");
                } else {
                    System.out.println("Failed to create DBFile, please restart");
                    return false;
                }
            }
            GroupsDB = DriverManager.getConnection("jdbc:sqlite:Data/Groups.db");
            Statement CreatingStatement = GroupsDB.createStatement();
            CreatingStatement.execute("CREATE TABLE IF NOT EXISTS Groups(id INTEGER PRIMARY KEY AUTOINCREMENT, ICN TEXT NOT NULL UNIQUE, DataPath TEXT NOT NULL UNIQUE)");
            CreatingStatement.close();
            System.out.println("DB has been created");
            return true;
        } catch (Exception SQLiteException) {
            System.out.println("Unknown file or sqlite error");
            return false;
        }
    }

    private static boolean LoadTimeModules() {
        System.out.println("Loading time modules...");
        try {
            DateForm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
            DateForm.setTimeZone(TimeZone.getTimeZone("GMT"));
            System.out.println("Time modules were successfully loaded in: " + DateForm.format(new Date()));
            return true;
        } catch (Exception TimeModulesException) {
            System.out.println("Unknown time module error, please restart");
        }
        return false;
    }

    private static boolean PrepareServer() {
        ConsoleIn = new Scanner(System.in);
        System.out.println("Welcome to RedTech33 & Ultima Server version: Demo_0.0.1 Community Edition");
        CreateInstruction();
        ServerConfigs = new Properties();
        if (!LoadServerConfigs()) {
            System.out.println("Failed to load configs");
            if (!CreateServerConfigs(ConsoleIn)) {
                System.out.println("Failed to create server configs, please try again");
                return false;
            }
        }
        if (!LoadClientsDB()) {
            System.out.println("Failed to load clients database");
            if (!CreateClientsDB()) {
                System.out.println("Failed to create clients database, please try again");
                return false;
            }
        }
        if (!LoadGroupsDB()) {
            System.out.println("Failed to load groups database");
            if (!CreateGroupsDB()) {
                System.out.println("Failed to create groups database, please try again");
                return false;
            }
        }
        if (!LoadTimeModules()) {
            System.out.println("Failed to load time modules, please try again");
            return false;
        }
        try {
            Server = new ServerSocket();
            Server.bind(new InetSocketAddress(Integer.parseInt(ServerConfigs.getProperty("Port", "39"))));
        } catch (Exception BindingException) {
            System.out.println("Failed to bind server, \nPlease try again");
            return false;
        }
        return true;
    }

    private static boolean MainLoop() {
        new Thread(() -> {
            while (isServerWorking) {
                try {
                    Socket NewClientSocket = Server.accept();
                    if (NewClientSocket != null) {
                        NewClientSocket.setSoTimeout(ServerTimeOut * 1000);
                        ClientThread NewClientThread = new ClientThread(NewClientSocket);
                        NewClientThread.setDaemon(true);
                        addClientThread();
                        NewClientThread.start();
                        System.out.println("Time: " + DateForm.format(new Date()) + " Client connected with ip: " + ByteOperations.GetStringByIp(NewClientSocket.getInetAddress().getAddress()) + ", port: " + NewClientSocket.getPort());
                    }

                } catch (Exception UnknownServerException) {
                    System.out.println("Failed to add client");
                }
            }
        }).start();

        boolean isConsoleWorking = isServerWorking;
        while (isConsoleWorking) {
            try {
                if (System.in.available() > 0) {
                    String cmd = ConsoleIn.nextLine();
                    switch (cmd) {
                        case "e" -> {
                            System.out.println("Closing...");
                            isServerWorking = false;
                            isConsoleWorking = false;
                            ClientsDB.close();
                            GroupsDB.close();
                            Server.close();
                            System.out.println("Successfully closed");
                            return true;
                        }
                        case "o" -> {
                            System.out.println("Online: " + Online_Users_Count);
                        }
                        case "p" -> {
                            System.out.println("Preparing to close");
                            isServerWorking = false;
                            System.out.println("Successfully prepared, check online users count");
                        }
                        default -> System.out.println("Unknown symbol");
                    }
                }
            } catch (Exception UnknownLoopException) {
                System.out.println("Unknown error");
            }
        }
        return false;
    }

    public static boolean Start() {
        if (!PrepareServer()) return false;
        System.out.println("Server started successfully");
        return MainLoop();
    }
}
