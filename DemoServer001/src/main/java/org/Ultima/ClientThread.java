package org.Ultima;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.Date;

/***
 * Client Thread class for Server version Demo_0.0.1
 ***/

public class ClientThread extends Thread {
    private Socket ClientSocket;
    private OutputStream ClientOut = null;
    private InputStream ClientIn = null;
    private long SecondsToClose;
    private Timer TimerToCloseThread = null;
    private boolean IsAuthorized = false;
    private volatile long ClientId;

    private String GetClientNetState() {
        return "ip: " + ByteOperations.GetStringByIp(ClientSocket.getInetAddress().getAddress()) + ", port: " + ClientSocket.getPort();
    }

    private synchronized void SkipSecond() {
        SecondsToClose--;
    }

    private boolean StopProcess() {
        try {
            System.out.println("Time: " + MainServer.DateForm.format(new Date()) + " Client disconnected ip: " + ByteOperations.GetStringByIp(ClientSocket.getInetAddress().getAddress()) + ", port: " + ClientSocket.getPort());
            MainServer.removeClientThread();
            if (ClientOut != null) ClientOut.close();
            if (ClientIn != null) ClientIn.close();
            if (TimerToCloseThread != null) TimerToCloseThread.cancel();
            if (ClientSocket != null) ClientSocket.close();
            ClientOut = null;
            ClientIn = null;
            TimerToCloseThread = null;
            ClientSocket = null;
        } catch (Exception DisconnectException) {
            return false;
        }
        return true;
    }

    private boolean PrepareThread() {
        if (ClientSocket != null) {
            SecondsToClose = MainServer.ServerTimeOut;
            TimerToCloseThread = new Timer();
            TimerToCloseThread.schedule(new TimerTask() {
                @Override
                public void run() {
                    SkipSecond();
                }
            }, 0, 1000);
            try {
                ClientOut = ClientSocket.getOutputStream();
                ClientIn = ClientSocket.getInputStream();
            } catch (Exception UnknownException) {
                return false;
            }
            return true;
        }
        return false;
    }

    private byte[] ReadByte() {
        try {
            while (!(ClientIn.available() > 0)) {
                if (!(MainServer.isServerWorking && SecondsToClose > 0)) {
                    return null;
                }
            }
            byte[] result = new byte[1];
            if (ClientIn.read(result) == 1) {
                SecondsToClose = MainServer.ServerTimeOut;
                return result;
            } else return null;
        } catch (Exception UnknownException) {
            return null;
        }
    }

    private byte[] ReadIntString() {
        try {
            int ReadIntStringLen = 0;
            while (ReadIntStringLen < 4) {
                if (ClientIn.available() > ReadIntStringLen) {
                    ReadIntStringLen = ClientIn.available();
                    SecondsToClose = MainServer.ServerTimeOut;
                }
                if (!(MainServer.isServerWorking && SecondsToClose > 0)) return null;
            }
            byte[] IntStringLen = new byte[4];
            if (ClientIn.read(IntStringLen) != 4) return null;
            SecondsToClose = MainServer.ServerTimeOut;
            byte[] IntString = new byte[ByteOperations.BytesToInt(IntStringLen)];
            int ReadIntStringBytes = 0;
            while (ReadIntStringBytes < ByteOperations.BytesToInt(IntStringLen)) {
                if (ClientIn.available() > ReadIntStringBytes) {
                    ReadIntStringBytes = ClientIn.available();
                    SecondsToClose = MainServer.ServerTimeOut;
                }
                if (!(MainServer.isServerWorking && SecondsToClose > 0)) return null;
            }
            if (ClientIn.read(IntString) != ByteOperations.BytesToInt(IntStringLen)) return null;
            SecondsToClose = MainServer.ServerTimeOut;
            return IntString;
        } catch (Exception UnknownException) {
            return null;
        }
    }

    private byte[] ReadInt() {
        try {
            int ReadIntBytes = 0;
            while (ReadIntBytes < 4) {
                if (ClientIn.available() > ReadIntBytes) {
                    ReadIntBytes = ClientIn.available();
                    SecondsToClose = MainServer.ServerTimeOut;
                }
                if (!(MainServer.isServerWorking && SecondsToClose > 0)) return null;
            }
            byte[] Result = new byte[4];
            if (ClientIn.read(Result) != 4) return null;
            SecondsToClose = MainServer.ServerTimeOut;
            return Result;
        } catch (Exception UnknownException) {
            return null;
        }
    }

    private byte[] ReadLong() {
        try {
            int ReadLongBytes = 0;
            while (ReadLongBytes < 8) {
                if (ClientIn.available() > ReadLongBytes) {
                    ReadLongBytes = ClientIn.available();
                    SecondsToClose = MainServer.ServerTimeOut;
                }
                if (!(MainServer.isServerWorking && SecondsToClose > 0)) return null;
            }
            byte[] Result = new byte[8];
            if (ClientIn.read(Result) != 8) return null;
            SecondsToClose = MainServer.ServerTimeOut;
            return Result;
        } catch (Exception UnknownException) {
            return null;
        }
    }

    private boolean WriteByte(byte Data) {
        try {
            ClientOut.write(Data);
            SecondsToClose = MainServer.ServerTimeOut;
            return true;
        } catch (Exception UnknownException) {
            return false;
        }
    }

    private boolean WriteInt(int Data) {
        try {
            ClientOut.write(ByteOperations.IntToBytes(Data));
            SecondsToClose = MainServer.ServerTimeOut;
            return true;
        } catch (Exception UnknownException) {
            return false;
        }
    }

    private boolean WriteLong(long Data) {
        try {
            ClientOut.write(ByteOperations.LongToBytes(Data));
            SecondsToClose = MainServer.ServerTimeOut;
            return true;
        } catch (Exception UnknownException) {
            return false;
        }
    }

    private boolean WriteIntString(byte[] Data) {
        try {
            ClientOut.write(ByteOperations.IntToBytes(Data.length));
            ClientOut.write(Data);
            SecondsToClose = MainServer.ServerTimeOut;
            return true;
        } catch (Exception UnknownException) {
            return false;
        }
    }

    private boolean ClientHandShake() {
        try {
            boolean isPasswordNeed = MainServer.ServerConfigs.getProperty("IsPasswordNeed", "false").equals("true");
            byte[] id;
            id = new byte[]{51, 57, Codes.VERSION, (isPasswordNeed) ? Codes.BASIC_YES : Codes.BASIC_NO};
            ClientOut.write(id);
            byte[] result = ReadByte();
            if (result == null) return false;
            if (result[0] == Codes.BASIC_YES) {
                if (isPasswordNeed) {
                    byte[] receivedPassword = ReadIntString();
                    if (receivedPassword == null) return false;
                    if (!(ByteOperations.Get_String_UTF_8(receivedPassword).equals(MainServer.ServerConfigs.getProperty("Password", "")))) {
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                }
                if (!WriteByte(Codes.BASIC_OK)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                return true;
            } else if (result[0] == Codes.BASIC_NO) {
                if (!WriteByte(Codes.BASIC_OK)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            } else {
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } catch (Exception UnknownException) {
            return false;
        }
    }

    private boolean UpdateOnlineDate() {
        if (IsAuthorized) {
            String DataPath = GetClientDataPath(ClientId);
            try {
                FileInputStream InfoReader = new FileInputStream(DataPath + "/Info.data");
                byte[] Status = new byte[4];
                if (InfoReader.read(Status) != 4) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    return false;
                }
                byte[] RegDateLen = new byte[4];
                if (InfoReader.read(RegDateLen) != 4) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    return false;
                }
                byte[] RegisterDate = new byte[ByteOperations.BytesToInt(RegDateLen)];
                if (InfoReader.read(RegisterDate) != ByteOperations.BytesToInt(RegDateLen)) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    return false;
                }
                InfoReader.close();
                if (!(new File(DataPath + "/Info.data").delete())) {
                    System.out.println("Can not delete old data in info.data client " + GetClientNetState());
                    return false;
                }
                if (!(new File(DataPath + "/Info.data").createNewFile())) {
                    System.out.println("Can not create new data in info.data client " + GetClientNetState());
                    return false;
                }
                FileOutputStream InfoWriter = new FileOutputStream(DataPath + "/Info.data");
                InfoWriter.write(Status);
                InfoWriter.write(RegDateLen);
                InfoWriter.write(RegisterDate);
                InfoWriter.write(ByteOperations.Get_IntString_UTF_8(MainServer.DateForm.format(new Date())));
                InfoWriter.close();
                SecondsToClose = MainServer.ServerTimeOut;
                return true;
            } catch (Exception UnknownException) {
                System.out.println("Failed to update online date for client " + GetClientNetState());
            }
        }
        return false;
    }

    private boolean ExecutePing() {
        if (!WriteByte(Codes.BASIC_OK)) {
            System.out.println("Failed to send response to client " + GetClientNetState());
        }
        if (IsAuthorized) {
            if (!UpdateOnlineDate())
                System.out.println("Failed to update online date for client " + GetClientNetState());
        }
        return true;
    }

    private boolean ExecuteDisconnect() {
        if (!WriteByte(Codes.BASIC_OK)) {
            System.out.println("Failed to send response to client " + GetClientNetState());
            return false;
        }
        if (IsAuthorized) {
            if (!UpdateOnlineDate())
                System.out.println("Failed to update online date for client " + GetClientNetState());
        }
        return true;
    }

    private Boolean IsClientICNTaken(byte[] ICN) {
        try {
            Boolean FinalResult = null;
            PreparedStatement CheckStatement = MainServer.ClientsDB.prepareStatement("SELECT COUNT(id) FROM Clients WHERE ICN = ?");
            CheckStatement.setString(1, ByteOperations.Get_String_UTF_8(ICN));
            ResultSet CheckResult = CheckStatement.executeQuery();
            if (CheckResult.next()) {
                FinalResult = (CheckResult.getInt(1) != 0);
            }
            CheckResult.close();
            CheckStatement.close();
            return FinalResult;
        } catch (Exception UnknownException) {
            return null;
        }
    }

    private Boolean IsGroupICNTaken(byte[] ICN) {
        try {
            Boolean FinalResult = null;
            PreparedStatement CheckStatement = MainServer.GroupsDB.prepareStatement("SELECT COUNT(id) FROM Groups WHERE ICN = ?");
            CheckStatement.setString(1, ByteOperations.Get_String_UTF_8(ICN));
            ResultSet CheckResult = CheckStatement.executeQuery();
            if (CheckResult.next()) {
                FinalResult = (CheckResult.getInt(1) != 0);
            }
            CheckResult.close();
            CheckStatement.close();
            return FinalResult;
        } catch (Exception UnknownException) {
            return null;
        }
    }

    private Boolean IsEmailTaken(byte[] Email) {
        try {
            Boolean FinalResult = null;
            PreparedStatement CheckStatement = MainServer.ClientsDB.prepareStatement("SELECT COUNT(id) FROM Clients WHERE Email = ?");
            CheckStatement.setString(1, ByteOperations.Get_String_UTF_8(Email));
            ResultSet CheckResult = CheckStatement.executeQuery();
            if (CheckResult.next()) {
                FinalResult = (CheckResult.getInt(1) != 0);
            }
            CheckResult.close();
            CheckStatement.close();
            return FinalResult;
        } catch (Exception UnknownException) {
            return null;
        }
    }

    private byte[] GetClientId(byte[] ICN) {
        try {
            PreparedStatement SearchStatement = MainServer.ClientsDB.prepareStatement("SELECT id FROM Clients WHERE ICN = ?");
            SearchStatement.setString(1, ByteOperations.Get_String_UTF_8(ICN));
            ResultSet ResultId = SearchStatement.executeQuery();
            if (!ResultId.next()) return null;
            long Id = ResultId.getLong(1);
            ResultId.close();
            SearchStatement.close();
            return ByteOperations.LongToBytes(Id);
        } catch (Exception UnknownException) {
            System.out.println("Failed to find client id for client " + GetClientNetState());
            return null;
        }
    }

    private byte[] GetClientICN(byte[] ClientId) {
        try {
            PreparedStatement SearchStatement = MainServer.ClientsDB.prepareStatement("SELECT ICN FROM Clients WHERE id = ?");
            SearchStatement.setLong(1, ByteOperations.BytesToLong(ClientId));
            ResultSet ResultICN = SearchStatement.executeQuery();
            if (!ResultICN.next()) return null;
            byte[] ICN = ByteOperations.Get_Bytes_By_String_UTF_8(ResultICN.getString(1));
            ResultICN.close();
            SearchStatement.close();
            return ICN;
        } catch (Exception UnknownException) {
            System.out.println("Failed to find client ICN for client " + GetClientNetState());
            return null;
        }
    }

    private byte[] GetGroupICN(byte[] GroupId) {
        try {
            PreparedStatement SearchStatement = MainServer.GroupsDB.prepareStatement("SELECT ICN FROM Groups WHERE id = ?");
            SearchStatement.setLong(1, ByteOperations.BytesToLong(GroupId));
            ResultSet ResultICN = SearchStatement.executeQuery();
            if (!ResultICN.next()) return null;
            byte[] ICN = ByteOperations.Get_Bytes_By_String_UTF_8(ResultICN.getString(1));
            ResultICN.close();
            SearchStatement.close();
            return ICN;
        } catch (Exception UnknownException) {
            System.out.println("Failed to find group ICN for client " + GetClientNetState());
            return null;
        }
    }

    private byte[] GetClientEmail(byte[] ClientId) {
        try {
            PreparedStatement SearchStatement = MainServer.ClientsDB.prepareStatement("SELECT Email FROM Clients WHERE id = ?");
            SearchStatement.setLong(1, ByteOperations.BytesToLong(ClientId));
            ResultSet ResultEmail = SearchStatement.executeQuery();
            if (!ResultEmail.next()) return null;
            byte[] Email = ByteOperations.Get_Bytes_By_String_UTF_8(ResultEmail.getString(1));
            ResultEmail.close();
            SearchStatement.close();
            return Email;
        } catch (Exception UnknownException) {
            System.out.println("Failed to find client Email for client " + GetClientNetState());
            return null;
        }
    }

    private byte[] GetGroupId(byte[] ICN) {
        try {
            PreparedStatement SearchStatement = MainServer.GroupsDB.prepareStatement("SELECT id FROM Groups WHERE ICN = ?");
            SearchStatement.setString(1, ByteOperations.Get_String_UTF_8(ICN));
            ResultSet ResultId = SearchStatement.executeQuery();
            if (!ResultId.next()) return null;
            long Id = ResultId.getLong(1);
            ResultId.close();
            SearchStatement.close();
            return ByteOperations.LongToBytes(Id);
        } catch (Exception UnknownException) {
            System.out.println("Failed to find group id for client " + GetClientNetState());
            return null;
        }
    }

    private String GetClientDataPath(long Id) {
        try {
            PreparedStatement SearchStatement = MainServer.ClientsDB.prepareStatement("SELECT DataPath FROM Clients WHERE id = ?");
            SearchStatement.setLong(1, Id);
            ResultSet ResultPath = SearchStatement.executeQuery();
            if (!ResultPath.next()) return null;
            String DataPath = ResultPath.getString(1);
            ResultPath.close();
            SearchStatement.close();
            return DataPath;
        } catch (Exception UnknownException) {
            System.out.println("Failed to find data path for client " + GetClientNetState());
            return null;
        }
    }

    private boolean ExecuteLogOut() {
        if (IsAuthorized) {
            if (!UpdateOnlineDate())
                System.out.println("Failed to update online date for client " + GetClientNetState());
            IsAuthorized = false;
            ClientId = 0;
            if (!WriteByte(Codes.BASIC_OK)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return true;
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private Boolean IsPasswordCorrect(byte[] ICN, byte[] Password) {
        Boolean FinalResult = null;
        try {
            PreparedStatement CheckStatement = MainServer.ClientsDB.prepareStatement("SELECT Password FROM Clients WHERE ICN = ?");
            CheckStatement.setString(1, ByteOperations.Get_String_UTF_8(ICN));
            ResultSet CheckResult = CheckStatement.executeQuery();
            if (!CheckResult.next()) {
                System.out.println("Can not read response from Clients.db to check password for client " + GetClientNetState());
                return null;
            }
            FinalResult = CheckResult.getString(1).equals(ByteOperations.Get_String_UTF_8(Password));
            CheckResult.close();
            CheckStatement.close();
        } catch (Exception UnknownException) {
            System.out.println("Failed to check password for client " + GetClientNetState());
        }
        return FinalResult;
    }

    private boolean ExecuteRegister() {
        if (MainServer.ServerConfigs.getProperty("CanRegister", "false").equals("true")) {
            try {
                if (!WriteByte(Codes.BASIC_YES)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                byte[] ICN = ReadIntString();
                byte[] Password = ReadIntString();
                byte[] Email = ReadIntString();
                byte[] Description = ReadIntString();
                if (ICN != null && Password != null && Email != null && Description != null) {
                    Boolean IsICNTakenResult = IsClientICNTaken(ICN);
                    Boolean IsEmailTakenResult = IsEmailTaken(Email);
                    if (IsICNTakenResult == null) {
                        System.out.println("Failed to get (is ICN taken) from db for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (IsEmailTakenResult == null) {
                        System.out.println("Failed to get (is Email taken) from db for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (IsICNTakenResult) {
                        if (!WriteByte(Codes.REG_RESULT_ERR_ICN_TAKEN)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (IsEmailTakenResult) {
                        if (!WriteByte(Codes.REG_RESULT_ERR_EMAIL_TAKEN)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    String DataPath = "Data/ClientsData/" + ByteOperations.Get_String_UTF_8(ICN);
                    if (!(new File(DataPath).mkdirs())) {
                        System.out.println("Failed to create data directory for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/ClientData").mkdirs())) {
                        System.out.println("Failed to create additional data directory for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/ClientData/ClientFiles").mkdirs())) {
                        System.out.println("Failed to create client's files directory for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/Info.data").createNewFile())) {
                        System.out.println("Failed to create Info.data for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/Description.data").createNewFile())) {
                        System.out.println("Failed to create Description.data for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    FileOutputStream DescriptionWriter = new FileOutputStream(DataPath + "/Description.data");
                    DescriptionWriter.write(ByteOperations.IntToBytes(Description.length));
                    DescriptionWriter.write(Description);
                    DescriptionWriter.close();
                    FileOutputStream InfoWriter = new FileOutputStream(DataPath + "/Info.data");
                    InfoWriter.write(ByteOperations.IntToBytes(Integer.parseInt(MainServer.ServerConfigs.getProperty("DefaultAccountStatus", "1"))));
                    InfoWriter.write(ByteOperations.Get_IntString_UTF_8(MainServer.DateForm.format(new Date())));
                    InfoWriter.write(ByteOperations.Get_IntString_UTF_8(MainServer.DateForm.format(new Date())));
                    InfoWriter.close();
                    PreparedStatement CreateClient = MainServer.ClientsDB.prepareStatement("INSERT INTO Clients (Email, ICN, Password, DataPath) VALUES (?, ?, ?, ?)");
                    CreateClient.setString(1, ByteOperations.Get_String_UTF_8(Email));
                    CreateClient.setString(2, ByteOperations.Get_String_UTF_8(ICN));
                    CreateClient.setString(3, ByteOperations.Get_String_UTF_8(Password));
                    CreateClient.setString(4, DataPath);
                    int Added = CreateClient.executeUpdate();
                    if (Added == 0) {
                        System.out.println("Client " + GetClientNetState() + " was not register");
                        CreateClient.close();
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    CreateClient.close();
                    SecondsToClose = MainServer.ServerTimeOut;
                    if (!WriteByte(Codes.BASIC_OK)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    byte[] Id = GetClientId(ICN);
                    if (Id == null) {
                        System.out.println("Failed to find client id while registering client " + GetClientNetState());
                        return false;
                    }
                    ClientId = ByteOperations.BytesToLong(Id);
                    IsAuthorized = true;
                    return true;
                } else {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
            } catch (Exception UnknownException) {
                System.out.println("Fatal exception while register client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.BASIC_NO)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            return false;
        }
    }

    private boolean ExecuteLogIn() {
        try {
            byte[] ICN = ReadIntString();
            byte[] Password = ReadIntString();
            if (ICN != null && Password != null) {
                Boolean IsPassCorrect = IsPasswordCorrect(ICN, Password);
                if (IsPassCorrect == null) return false;
                if (IsPassCorrect) {
                    SecondsToClose = MainServer.ServerTimeOut;
                    if (!WriteByte(Codes.BASIC_OK)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    byte[] Id = GetClientId(ICN);
                    if (Id == null) {
                        System.out.println("Failed to find client id while login client " + GetClientNetState());
                        return false;
                    }
                    ClientId = ByteOperations.BytesToLong(Id);
                    IsAuthorized = true;
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                } else {
                    System.out.println("Client " + GetClientNetState() + " wrote password wrongly");
                    if (!WriteByte(Codes.BASIC_ERR)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
            } else {
                System.out.println("Failed to read data from client " + GetClientNetState());
                return false;
            }
        } catch (Exception UnknownException) {
            System.out.println("Fatal exception while login client " + GetClientNetState());
            if (!WriteByte(Codes.BASIC_ERR)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetIp() {
        try {
            if (!WriteByte(Codes.BASIC_OK)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (!WriteIntString(ClientSocket.getInetAddress().getAddress())) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (IsAuthorized) {
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
            }
            return true;
        } catch (Exception UnknownException) {
            System.out.println("Failed to send client ip to client " + GetClientNetState());
            if (!WriteByte(Codes.BASIC_ERR)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetDate() {
        try {
            if (!WriteByte(Codes.BASIC_OK)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (!WriteIntString(ByteOperations.Get_Bytes_By_String_UTF_8(MainServer.DateForm.format(new Date())))) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (IsAuthorized) {
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
            }
            return true;
        } catch (Exception UnknownException) {
            System.out.println("Failed to send current date to client " + GetClientNetState());
            if (!WriteByte(Codes.BASIC_ERR)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetServerInfo() {
        try {
            if (!WriteByte(Codes.BASIC_OK)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (!WriteInt(MainServer.ServerTimeOut)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (!WriteIntString(ByteOperations.Get_Bytes_By_String_UTF_8(MainServer.ServerConfigs.getProperty("Name", "")))) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (!WriteIntString(ByteOperations.Get_Bytes_By_String_UTF_8(MainServer.ServerConfigs.getProperty("Description", "")))) {
                System.out.println("Failed to send response to client " + GetClientNetState());
                return false;
            }
            if (IsAuthorized) {
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
            }
            return true;
        } catch (Exception UnknownException) {
            System.out.println("Failed to send server info to client " + GetClientNetState());
            if (!WriteByte(Codes.BASIC_ERR)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteCreateGroup() {
        if (IsAuthorized) {
            try {
                if (!WriteByte(Codes.BASIC_OK)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                byte[] ICN = ReadIntString();
                byte[] Description = ReadIntString();
                byte[] DefaultMemberStatus = ReadInt();
                byte[] MembersCount = ReadInt();
                byte[] AdminId = ReadLong();
                if (ICN != null && Description != null && DefaultMemberStatus != null && MembersCount != null && AdminId != null) {
                    long[] MemberIds = new long[ByteOperations.BytesToInt(MembersCount) - 1];
                    for (int ReadMembersId = 0; ReadMembersId < (ByteOperations.BytesToInt(MembersCount) - 1); ReadMembersId++) {
                        byte[] ReadId = ReadLong();
                        if (ReadId != null) MemberIds[ReadMembersId] = ByteOperations.BytesToLong(ReadId);
                        else {
                            System.out.println("Failed to read data from client " + GetClientNetState());
                            return false;
                        }
                    }
                    Boolean IsICNTakenResult = IsGroupICNTaken(ICN);
                    if (IsICNTakenResult == null) {
                        System.out.println("Failed to get (is ICN taken) from group db for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (IsICNTakenResult) {
                        if (!WriteByte(Codes.GR_RESULT_ERR_ICN_TAKEN)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    String DataPath = "Data/GroupsData/" + ByteOperations.Get_String_UTF_8(ICN);
                    if (!(new File(DataPath).mkdirs())) {
                        System.out.println("Failed to create group data directory for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/GroupData").mkdirs())) {
                        System.out.println("Failed to create additional group data directory for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/GroupData/GroupFiles").mkdirs())) {
                        System.out.println("Failed to create group's files directory for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/Info.data").createNewFile())) {
                        System.out.println("Failed to create group Info.data for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/Description.data").createNewFile())) {
                        System.out.println("Failed to create group Description.data for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/Members.db").createNewFile())) {
                        System.out.println("Failed to create group members.db for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/MessagesData").mkdirs())) {
                        System.out.println("Failed to create group messages directory for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    if (!(new File(DataPath + "/Messages.db").createNewFile())) {
                        System.out.println("Failed to create group Messages.db for client " + GetClientNetState());
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    FileOutputStream DescriptionWriter = new FileOutputStream(DataPath + "/Description.data");
                    DescriptionWriter.write(ByteOperations.IntToBytes(Description.length));
                    DescriptionWriter.write(Description);
                    DescriptionWriter.close();
                    Connection CreateMDBConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Members.db");
                    Statement LoadingMDBStatement = CreateMDBConnection.createStatement();
                    LoadingMDBStatement.execute("CREATE TABLE IF NOT EXISTS Members(ClientId INTEGER PRIMARY KEY, Status INTEGER NOT NULL)");
                    LoadingMDBStatement.close();
                    PreparedStatement AddAdmin = CreateMDBConnection.prepareStatement("INSERT INTO Members(ClientId, Status) VALUES (?, ?)");
                    AddAdmin.setLong(1, ByteOperations.BytesToLong(AdminId));
                    AddAdmin.setInt(2, 0);
                    int AdminsAdded = AddAdmin.executeUpdate();
                    if (AdminsAdded == 0) {
                        System.out.println("Failed to create new member in Members.db for client " + GetClientNetState());
                        AddAdmin.close();
                        CreateMDBConnection.close();
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    AddAdmin.close();
                    for (int WriteMembers = 0; WriteMembers < (ByteOperations.BytesToInt(MembersCount) - 1); WriteMembers++) {
                        PreparedStatement AddMember = CreateMDBConnection.prepareStatement("INSERT INTO Members(ClientId, Status) VALUES (?, ?)");
                        AddMember.setLong(1, MemberIds[WriteMembers]);
                        AddMember.setInt(2, ByteOperations.BytesToInt(DefaultMemberStatus));
                        int MembersAdded = AddMember.executeUpdate();
                        if (MembersAdded == 0) {
                            System.out.println("Failed to create new member in Members.db for client " + GetClientNetState());
                            AddMember.close();
                            CreateMDBConnection.close();
                            if (!WriteByte(Codes.BASIC_ERR)) {
                                System.out.println("Failed to send response to client " + GetClientNetState());
                            }
                            return false;
                        }
                        AddMember.close();
                    }
                    CreateMDBConnection.close();
                    Connection CreateMSGDBConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Messages.db");
                    Statement LoadingMSGDBStatement = CreateMSGDBConnection.createStatement();
                    LoadingMSGDBStatement.execute("CREATE TABLE IF NOT EXISTS Messages(id INTEGER PRIMARY KEY AUTOINCREMENT, SenderId INTEGER NOT NULL, DataPath TEXT NOT NULL UNIQUE, SendDate TEXT NOT NULL)");
                    LoadingMSGDBStatement.close();
                    CreateMSGDBConnection.close();
                    FileOutputStream InfoWriter = new FileOutputStream(DataPath + "/Info.data");
                    InfoWriter.write(DefaultMemberStatus);
                    InfoWriter.write(ByteOperations.Get_IntString_UTF_8(MainServer.DateForm.format(new Date())));
                    InfoWriter.close();
                    PreparedStatement CreateGroup = MainServer.GroupsDB.prepareStatement("INSERT INTO Groups (ICN, DataPath) VALUES (?, ?)");
                    CreateGroup.setString(1, ByteOperations.Get_String_UTF_8(ICN));
                    CreateGroup.setString(2, DataPath);
                    int Added = CreateGroup.executeUpdate();
                    if (Added == 0) {
                        System.out.println("Failed to create group for client " + GetClientNetState());
                        CreateGroup.close();
                        if (!WriteByte(Codes.BASIC_ERR)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    CreateGroup.close();
                    SecondsToClose = MainServer.ServerTimeOut;
                    if (!WriteByte(Codes.BASIC_OK)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                } else {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to create group client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private Integer GetMemberStatus(long GroupId, long MemberId) {
        try {
            String DataPath = GetGroupDataPath(GroupId);
            if (DataPath == null) return null;
            Connection SearchConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Members.db");
            PreparedStatement SearchStatement = SearchConnection.prepareStatement("SELECT Status FROM Members WHERE ClientId = ?");
            SearchStatement.setLong(1, MemberId);
            ResultSet ResultStatus = SearchStatement.executeQuery();
            if (!ResultStatus.next()) return null;
            int Status = ResultStatus.getInt(1);
            ResultStatus.close();
            SearchStatement.close();
            SearchConnection.close();
            return Status;
        } catch (Exception UnknownException) {
            System.out.println("Failed to find group status for client " + GetClientNetState());
            return null;
        }
    }

    private boolean ExecuteGetClientId() {
        if (IsAuthorized) {
            try {
                byte[] ICN = ReadIntString();
                if (ICN == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                byte[] ClientId = GetClientId(ICN);
                if (ClientId == null) {
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                } else {
                    if (!WriteByte(Codes.UNF_OK_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteLong(ByteOperations.BytesToLong(ClientId))) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to get client id for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetGroupId() {
        if (IsAuthorized) {
            try {
                byte[] ICN = ReadIntString();
                if (ICN == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                byte[] GroupId = GetGroupId(ICN);
                if (GroupId == null) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                } else {
                    if (!WriteByte(Codes.GR_OK_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteLong(ByteOperations.BytesToLong(GroupId))) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to get group id for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetGroupICN() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                if (GroupId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                byte[] ICN = GetGroupICN(GroupId);
                if (ICN == null) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                } else {
                    if (!WriteByte(Codes.GR_OK_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteIntString(ICN)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to get group ICN for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetMembers() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                if (GroupId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                String DataPath = GetGroupDataPath(ByteOperations.BytesToLong(GroupId));
                if (DataPath == null) {
                    System.out.println("Failed to get group data path, unknown group " + GetClientNetState());
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                if (GetMemberStatus(ByteOperations.BytesToLong(GroupId), ClientId) == null) {
                    System.out.println("Failed to get members, client is not group " + GetClientNetState());
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                Connection GetMemberIdsConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Members.db");
                List<Long> MemberIds = new ArrayList<>();
                Statement GetMemberIds = GetMemberIdsConnection.createStatement();
                ResultSet MemberIdsResult = GetMemberIds.executeQuery("SELECT ClientId FROM Members");
                while (MemberIdsResult.next()) {
                    MemberIds.add(MemberIdsResult.getLong(1));
                }
                MemberIdsResult.close();
                GetMemberIds.close();
                GetMemberIdsConnection.close();
                SecondsToClose = MainServer.ServerTimeOut;
                if (!WriteByte(Codes.GR_OK_FOUND)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    MemberIds.clear();
                    return false;
                }
                long MemberIdsCount = MemberIds.size();
                if (!WriteLong(MemberIdsCount)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    MemberIds.clear();
                    return false;
                }
                for (long SentIds = 0; SentIds < MemberIdsCount; SentIds++) {
                    if (!WriteLong(MemberIds.get((int) SentIds))) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        MemberIds.clear();
                        return false;
                    }
                }
                MemberIds.clear();
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
                return true;
            } catch (Exception UnknownException) {
                System.out.println("Failed to get members ids for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteAddMember() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                byte[] ClientId = ReadLong();
                byte[] ClientStatus = ReadInt();
                if (ClientId == null || GroupId == null || ClientStatus == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                Integer InviterStatus = GetMemberStatus(ByteOperations.BytesToLong(GroupId), ByteOperations.BytesToLong(ClientId));
                if (InviterStatus == null) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                }
                if (InviterStatus != 0) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                }
                String DataPath = GetGroupDataPath(ByteOperations.BytesToLong(GroupId));
                if (DataPath == null) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                }
                Connection AddMemberConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Members.db");
                PreparedStatement AddMember = AddMemberConnection.prepareStatement("INSERT INTO Members(ClientId, Status) VALUES (?, ?)");
                AddMember.setLong(1, ByteOperations.BytesToLong(ClientId));
                AddMember.setInt(2, ByteOperations.BytesToInt(ClientStatus));
                int MembersAdded = AddMember.executeUpdate();
                if (MembersAdded == 0) {
                    System.out.println("Failed to create new member in Members.db for client " + GetClientNetState());
                    AddMember.close();
                    AddMemberConnection.close();
                    if (!WriteByte(Codes.BASIC_ERR)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                AddMember.close();
                AddMemberConnection.close();
                if (!WriteByte(Codes.BASIC_OK)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
                return true;
            } catch (Exception UnknownException) {
                System.out.println("Failed to add member in group for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteRemoveMember() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                byte[] ClientId = ReadLong();
                if (ClientId == null || GroupId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                Integer InviterStatus = GetMemberStatus(ByteOperations.BytesToLong(GroupId), ByteOperations.BytesToLong(ClientId));
                if (InviterStatus == null) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                }
                if (InviterStatus != 0) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                }
                String DataPath = GetGroupDataPath(ByteOperations.BytesToLong(GroupId));
                if (DataPath == null) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                }
                Connection RemoveMemberConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Members.db");
                PreparedStatement RemoveMember = RemoveMemberConnection.prepareStatement("DELETE FROM Members WHERE ClientId = ?");
                RemoveMember.setLong(1, ByteOperations.BytesToLong(ClientId));
                int MembersRemoved = RemoveMember.executeUpdate();
                if (MembersRemoved == 0) {
                    System.out.println("Failed to remove member from Members.db for client " + GetClientNetState());
                    RemoveMember.close();
                    RemoveMemberConnection.close();
                    if (!WriteByte(Codes.BASIC_ERR)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                RemoveMember.close();
                RemoveMemberConnection.close();
                if (!WriteByte(Codes.BASIC_OK)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
                return true;
            } catch (Exception UnknownException) {
                System.out.println("Failed to remove member from group for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetMemberStatus() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                byte[] ClientId = ReadLong();
                if (ClientId == null || GroupId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                Integer Status = GetMemberStatus(ByteOperations.BytesToLong(GroupId), ByteOperations.BytesToLong(ClientId));
                if (Status == null) {
                    if (!WriteByte(Codes.GR_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                } else {
                    if (!WriteByte(Codes.GR_OK_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteInt(Status)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to get member status for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetMSGIds() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                if (GroupId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                String DataPath = GetGroupDataPath(ByteOperations.BytesToLong(GroupId));
                if (DataPath == null) {
                    System.out.println("Failed to get group data path, unknown group " + GetClientNetState());
                    if (!WriteByte(Codes.GM_ERR_NO_ACCESS)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                if (GetMemberStatus(ByteOperations.BytesToLong(GroupId), ClientId) == null) {
                    System.out.println("Failed to get message's ids, client is not in group " + GetClientNetState());
                    if (!WriteByte(Codes.GM_ERR_NO_ACCESS)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                Connection GetMSGIdsConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Messages.db");
                List<Long> MSGIds = new ArrayList<>();
                Statement GetMSGIds = GetMSGIdsConnection.createStatement();
                ResultSet MSGIdsResult = GetMSGIds.executeQuery("SELECT id FROM Messages ORDER BY id");
                while (MSGIdsResult.next()) {
                    MSGIds.add(MSGIdsResult.getLong(1));
                }
                MSGIdsResult.close();
                GetMSGIds.close();
                GetMSGIdsConnection.close();
                SecondsToClose = MainServer.ServerTimeOut;
                if (!WriteByte(Codes.GM_OK)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    MSGIds.clear();
                    return false;
                }
                long MSGIdsCount = MSGIds.size();
                if (!WriteLong(MSGIdsCount)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    MSGIds.clear();
                    return false;
                }
                for (long SentIds = 0; SentIds < MSGIdsCount; SentIds++) {
                    if (!WriteLong(MSGIds.get((int) SentIds))) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        MSGIds.clear();
                        return false;
                    }
                }
                MSGIds.clear();
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
                return true;
            } catch (Exception UnknownException) {
                System.out.println("Failed to get message's ids for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetClientInfo() {
        if (IsAuthorized) {
            try {
                byte[] ClientId = ReadLong();
                if (ClientId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                String DataPath = GetClientDataPath(ByteOperations.BytesToLong(ClientId));
                byte[] Email = GetClientEmail(ClientId);
                if (DataPath == null || Email == null) {
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                }
                FileInputStream DescriptionReader = new FileInputStream(DataPath + "/Description.data");
                byte[] DescriptionLen = new byte[4];
                if (DescriptionReader.read(DescriptionLen) != 4) {
                    System.out.println("Description.data has been corrupted client " + GetClientNetState());
                    DescriptionReader.close();
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                byte[] Description = new byte[ByteOperations.BytesToInt(DescriptionLen)];
                if (DescriptionReader.read(Description) != ByteOperations.BytesToInt(DescriptionLen)) {
                    System.out.println("Description.data has been corrupted client " + GetClientNetState());
                    DescriptionReader.close();
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                DescriptionReader.close();
                FileInputStream InfoReader = new FileInputStream(DataPath + "/Info.data");
                byte[] Status = new byte[4];
                if (InfoReader.read(Status) != 4) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                byte[] RegDateLen = new byte[4];
                if (InfoReader.read(RegDateLen) != 4) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                byte[] RegisterDate = new byte[ByteOperations.BytesToInt(RegDateLen)];
                if (InfoReader.read(RegisterDate) != ByteOperations.BytesToInt(RegDateLen)) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                byte[] LatDateLen = new byte[4];
                if (InfoReader.read(LatDateLen) != 4) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                byte[] LatDate = new byte[ByteOperations.BytesToInt(LatDateLen)];
                if (InfoReader.read(LatDate) != ByteOperations.BytesToInt(LatDateLen)) {
                    System.out.println("Info.data has been corrupted client " + GetClientNetState());
                    InfoReader.close();
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                InfoReader.close();
                SecondsToClose = MainServer.ServerTimeOut;
                if (!WriteByte(Codes.UNF_OK_FOUND)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!WriteInt(ByteOperations.BytesToInt(Status))) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!WriteIntString(Email)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!WriteIntString(Description)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!WriteIntString(RegisterDate)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!WriteIntString(LatDate)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                if (!UpdateOnlineDate())
                    System.out.println("Failed to update online date for client " + GetClientNetState());
                return true;
            } catch (Exception UnknownException) {
                System.out.println("Failed to get client info for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetClientICN() {
        if (IsAuthorized) {
            try {
                byte[] ClientId = ReadLong();
                if (ClientId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                byte[] ICN = GetClientICN(ClientId);
                if (ICN == null) {
                    if (!WriteByte(Codes.UNF_ERR_NOT_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return false;
                } else {
                    if (!WriteByte(Codes.UNF_OK_FOUND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteIntString(ICN)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to get client ICN for client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private String GetGroupDataPath(long GroupId) {
        try {
            PreparedStatement SearchStatement = MainServer.GroupsDB.prepareStatement("SELECT DataPath FROM Groups WHERE id = ?");
            SearchStatement.setLong(1, GroupId);
            ResultSet ResultPath = SearchStatement.executeQuery();
            if (!ResultPath.next()) return null;
            String DataPath = ResultPath.getString(1);
            ResultPath.close();
            SearchStatement.close();
            return DataPath;
        } catch (Exception UnknownException) {
            System.out.println("Failed to find group data path for client " + GetClientNetState());
            return null;
        }
    }

    private boolean ExecuteSendMessage() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                if (GroupId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                String DataPath = GetGroupDataPath(ByteOperations.BytesToLong(GroupId));
                if (DataPath == null) {
                    System.out.println("Failed to send message, unknown group " + GetClientNetState());
                    if (!WriteByte(Codes.SM_ERR_CAN_NOT_SEND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                if (GetMemberStatus(ByteOperations.BytesToLong(GroupId), ClientId) == null) {
                    System.out.println("Failed to send message, client is not in group " + GetClientNetState());
                    if (!WriteByte(Codes.SM_ERR_CAN_NOT_SEND)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                if (!WriteByte(Codes.SM_OK_CAN_SEND)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                    return false;
                }
                byte[] MSGType = ReadByte();
                if (MSGType != null) {
                    String MSGPath;
                    if (MSGType[0] == Codes.MSG_TYPE_TEXT) {
                        byte[] Data = ReadIntString();
                        if (Data == null) {
                            System.out.println("Failed to read data from client " + GetClientNetState());
                            if (!WriteByte(Codes.SM_MSG_WAS_NOT_SENT)) {
                                System.out.println("Failed to send response to client " + GetClientNetState());
                            }
                            return false;
                        }
                        MSGPath = DataPath + "/MessagesData/" + Long.toHexString(new Random().nextLong()) + ".msg";
                        while (new File(MSGPath).exists())
                            MSGPath = DataPath + "/MessagesData/" + Long.toHexString(new Random().nextLong()) + ".msg";
                        FileOutputStream MSGWriter = new FileOutputStream(MSGPath);
                        MSGWriter.write(MSGType);
                        MSGWriter.write(ByteOperations.IntToBytes(Data.length));
                        MSGWriter.write(Data);
                        MSGWriter.close();
                    } else if (MSGType[1] == Codes.MSG_TYPE_FILE) {
                        byte[] Name = ReadIntString();
                        byte[] Data = ReadIntString();
                        if (Name == null || Data == null) {
                            System.out.println("Failed to read data from client " + GetClientNetState());
                            if (!WriteByte(Codes.SM_MSG_WAS_NOT_SENT)) {
                                System.out.println("Failed to send response to client " + GetClientNetState());
                            }
                            return false;
                        }
                        MSGPath = DataPath + "/MessagesData/" + Long.toHexString(new Random().nextLong()) + ".msg";
                        while (new File(MSGPath).exists())
                            MSGPath = DataPath + "/MessagesData/" + Long.toHexString(new Random().nextLong()) + ".msg";
                        FileOutputStream MSGWriter = new FileOutputStream(MSGPath);
                        MSGWriter.write(MSGType);
                        MSGWriter.write(ByteOperations.IntToBytes(Name.length));
                        MSGWriter.write(Name);
                        MSGWriter.write(ByteOperations.IntToBytes(Data.length));
                        MSGWriter.write(Data);
                        MSGWriter.close();
                    } else {
                        System.out.println("Unknown message type client " + GetClientNetState());
                        if (!WriteByte(Codes.SM_MSG_WAS_NOT_SENT)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    Connection CreateMSGConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Messages.db");
                    PreparedStatement CreateMSGStatement = CreateMSGConnection.prepareStatement("INSERT INTO Messages(SenderId, DataPath, SendDate) VALUES (?, ?, ?)");
                    CreateMSGStatement.setLong(1, ClientId);
                    CreateMSGStatement.setString(2, MSGPath);
                    CreateMSGStatement.setString(3, MainServer.DateForm.format(new Date()));
                    int Result = CreateMSGStatement.executeUpdate();
                    if (Result == 0) {
                        System.out.println("Unknown messages db error client " + GetClientNetState());
                        if (!WriteByte(Codes.SM_MSG_WAS_NOT_SENT)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    CreateMSGStatement.close();
                    CreateMSGConnection.close();
                    SecondsToClose = MainServer.ServerTimeOut;
                    if (!WriteByte(Codes.SM_MSG_WAS_SENT)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                } else {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to send message client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ExecuteGetMessage() {
        if (IsAuthorized) {
            try {
                byte[] GroupId = ReadLong();
                byte[] MSGId = ReadLong();
                if (GroupId == null || MSGId == null) {
                    System.out.println("Failed to read data from client " + GetClientNetState());
                    return false;
                }
                String DataPath = GetGroupDataPath(ByteOperations.BytesToLong(GroupId));
                if (DataPath == null) {
                    System.out.println("Failed to get message, unknown group " + GetClientNetState());
                    if (!WriteByte(Codes.GM_ERR_NO_ACCESS)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                if (GetMemberStatus(ByteOperations.BytesToLong(GroupId), ClientId) == null) {
                    System.out.println("Failed to get message, client is not group " + GetClientNetState());
                    if (!WriteByte(Codes.GM_ERR_NO_ACCESS)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                Connection GetMSGConnection = DriverManager.getConnection("jdbc:sqlite:" + DataPath + "/Messages.db");
                PreparedStatement GetMSGStatement = GetMSGConnection.prepareStatement("SELECT * FROM Messages WHERE id = ?");
                GetMSGStatement.setLong(1, ByteOperations.BytesToLong(MSGId));
                ResultSet MSGInfoResult = GetMSGStatement.executeQuery();
                if (!MSGInfoResult.next()) {
                    System.out.println("Failed to get message, no such message " + GetClientNetState());
                    MSGInfoResult.close();
                    GetMSGStatement.close();
                    GetMSGConnection.close();
                    if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
                long SenderId = MSGInfoResult.getLong("SenderId");
                String MSGPath = MSGInfoResult.getString("DataPath");
                String SendDate = MSGInfoResult.getString("SendDate");
                MSGInfoResult.close();
                GetMSGStatement.close();
                GetMSGConnection.close();
                FileInputStream MSGReader = new FileInputStream(MSGPath);
                byte[] MSGType = new byte[1];
                if (MSGReader.read(MSGType) != 1) {
                    System.out.println("Message data has been corrupted client " + GetClientNetState());
                    if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    MSGReader.close();
                    return false;
                }
                if (MSGType[0] == Codes.MSG_TYPE_TEXT) {
                    byte[] DataLen = new byte[4];
                    if (MSGReader.read(DataLen) != 4) {
                        System.out.println("Message data has been corrupted client " + GetClientNetState());
                        MSGReader.close();
                        if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    byte[] Data = new byte[ByteOperations.BytesToInt(DataLen)];
                    if (MSGReader.read(Data) != ByteOperations.BytesToInt(DataLen)) {
                        System.out.println("Message data has been corrupted client " + GetClientNetState());
                        MSGReader.close();
                        if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    MSGReader.close();
                    SecondsToClose = MainServer.ServerTimeOut;
                    if (!WriteByte(Codes.GM_OK)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteLong(SenderId)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteIntString(ByteOperations.Get_Bytes_By_String_UTF_8(SendDate))) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteByte(MSGType[0])) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteIntString(Data)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                } else if (MSGType[0] == Codes.MSG_TYPE_FILE) {
                    byte[] NameLen = new byte[4];
                    if (MSGReader.read(NameLen) != 4) {
                        System.out.println("Message data has been corrupted client " + GetClientNetState());
                        MSGReader.close();
                        if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    byte[] Name = new byte[ByteOperations.BytesToInt(NameLen)];
                    if (MSGReader.read(Name) != ByteOperations.BytesToInt(NameLen)) {
                        System.out.println("Message data has been corrupted client " + GetClientNetState());
                        MSGReader.close();
                        if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    byte[] DataLen = new byte[4];
                    if (MSGReader.read(DataLen) != 4) {
                        System.out.println("Message data has been corrupted client " + GetClientNetState());
                        MSGReader.close();
                        if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    byte[] Data = new byte[ByteOperations.BytesToInt(DataLen)];
                    if (MSGReader.read(Data) != ByteOperations.BytesToInt(DataLen)) {
                        System.out.println("Message data has been corrupted client " + GetClientNetState());
                        MSGReader.close();
                        if (!WriteByte(Codes.GM_ERR_NO_SUCH_MSG)) {
                            System.out.println("Failed to send response to client " + GetClientNetState());
                        }
                        return false;
                    }
                    MSGReader.close();
                    SecondsToClose = MainServer.ServerTimeOut;
                    if (!WriteByte(Codes.GM_OK)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteLong(SenderId)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteIntString(ByteOperations.Get_Bytes_By_String_UTF_8(SendDate))) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteByte(MSGType[0])) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteIntString(Name)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!WriteIntString(Data)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                        return false;
                    }
                    if (!UpdateOnlineDate())
                        System.out.println("Failed to update online date for client " + GetClientNetState());
                    return true;
                } else {
                    System.out.println("Unknown message type, it can be corrupted " + GetClientNetState());
                    MSGReader.close();
                    if (!WriteByte(Codes.GM_ERR_UNKNOWN_MSG_TYPE)) {
                        System.out.println("Failed to send response to client " + GetClientNetState());
                    }
                    return false;
                }
            } catch (Exception UnknownException) {
                System.out.println("Failed to get message client " + GetClientNetState());
                if (!WriteByte(Codes.BASIC_ERR)) {
                    System.out.println("Failed to send response to client " + GetClientNetState());
                }
                return false;
            }
        } else {
            if (!WriteByte(Codes.ACCOUNT_WILL_BE_AUTHORIZED)) {
                System.out.println("Failed to send response to client " + GetClientNetState());
            }
            return false;
        }
    }

    private boolean ClientLoop() {
        while (MainServer.isServerWorking) {
            try {
                if (!(SecondsToClose > 0)) {
                    return false;
                }
                if (ClientIn.available() > 0) {
                    byte[] request = ReadByte();
                    if (request != null) {
                        switch (request[0]) {
                            case Codes.BASIC_PING:
                                ExecutePing();
                                break;
                            case Codes.BASIC_DISCONNECT:
                                return ExecuteDisconnect();
                            case Codes.BASIC_GET_SERVER_INFO:
                                if (ExecuteGetServerInfo())
                                    System.out.println("Client got server info successfully " + GetClientNetState());
                                break;
                            case Codes.BASIC_GET_IP:
                                if (ExecuteGetIp())
                                    System.out.println("Client got ip successfully " + GetClientNetState());
                                break;
                            case Codes.BASIC_GET_DATE:
                                if (ExecuteGetDate())
                                    System.out.println("Client got date successfully " + GetClientNetState());
                                break;
                            case Codes.ACCOUNT_REG:
                                if (ExecuteRegister())
                                    System.out.println("Client successfully registered " + GetClientNetState());
                                break;
                            case Codes.ACCOUNT_LOGIN:
                                if (ExecuteLogIn())
                                    System.out.println("Client successfully logged in " + GetClientNetState());
                                break;
                            case Codes.ACCOUNT_LOGOUT:
                                if (ExecuteLogOut())
                                    System.out.println("Client logged out successfully" + GetClientNetState());
                                break;
                            case Codes.GR_CREATE_GROUP:
                                if (ExecuteCreateGroup())
                                    System.out.println("Client create new group " + GetClientNetState());
                                break;
                            case Codes.UNF_GET_ID_BY_ICN:
                                if (ExecuteGetClientId())
                                    System.out.println("Client get client id successfully " + GetClientNetState());
                                break;
                            case Codes.SM_SEND_MSG:
                                if (ExecuteSendMessage())
                                    System.out.println("Client sent message successfully " + GetClientNetState());
                                break;
                            case Codes.GM_GET_MSG:
                                if (ExecuteGetMessage())
                                    System.out.println("Client got message successfully " + GetClientNetState());
                                break;
                            case Codes.UNF_GET_ICN:
                                if (ExecuteGetClientICN())
                                    System.out.println("Client got client ICN successfully " + GetClientNetState());
                                break;
                            case Codes.UNF_GET_INFO:
                                if (ExecuteGetClientInfo())
                                    System.out.println("Client got client info successfully " + GetClientNetState());
                                break;
                            case Codes.GM_GET_MSG_IDS:
                                if (ExecuteGetMSGIds())
                                    System.out.println("Client got message's ids successfully " + GetClientNetState());
                                break;
                            case Codes.GR_ADD_MEMBER:
                                if (ExecuteAddMember())
                                    System.out.println("Client added member successfully " + GetClientNetState());
                                break;
                            case Codes.GR_REMOVE_MEMBER:
                                if (ExecuteRemoveMember())
                                    System.out.println("Client removed member successfully " + GetClientNetState());
                                break;
                            case Codes.GR_GET_MEMBERS:
                                if (ExecuteGetMembers())
                                    System.out.println("Client got members successfully " + GetClientNetState());
                                break;
                            case Codes.GR_GET_MEMBER_STATUS:
                                if (ExecuteGetMemberStatus())
                                    System.out.println("Client got member status successfully " + GetClientNetState());
                                break;
                            case Codes.GR_GET_ID:
                                if (ExecuteGetGroupId())
                                    System.out.println("Client got group id successfully " + GetClientNetState());
                                break;
                            case Codes.GR_GET_ICN:
                                if (ExecuteGetGroupICN())
                                    System.out.println("Client got group ICN successfully " + GetClientNetState());
                                break;
                            default:
                                if (!WriteByte(Codes.BASIC_ERR)) {
                                    System.out.println("Failed to send response to client " + GetClientNetState());
                                    return false;
                                }
                                break;
                        }
                    } else return false;
                }
            } catch (Exception UnknownException) {
                System.out.println("Client " + GetClientNetState() + " generate an exception");
            }
        }
        return true;
    }

    public ClientThread(Socket Socket) {
        ClientSocket = Socket;
    }

    @Override
    public void run() {
        super.run();
        try {
            if (!PrepareThread()) System.out.println("Can not prepare thread for client " + GetClientNetState());
            if (!ClientHandShake()) {
                if (!StopProcess()) System.out.println("Client " + GetClientNetState() + " disconnected with error");
                return;
            }
            if (!ClientLoop()) System.out.println("Client " + GetClientNetState() + " generate a fatal exception");
            if (!StopProcess()) System.out.println("Client " + GetClientNetState() + " disconnected with error");
        } catch (Exception UnknownException) {
            System.out.println("Unknown exception with client " + GetClientNetState());
        }
    }
}
