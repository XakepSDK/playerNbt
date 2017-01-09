package dk.xakeps.bukkit.playernbt;

import me.dpohvar.powernbt.PowerNBT;
import me.dpohvar.powernbt.api.NBTCompound;
import me.dpohvar.powernbt.api.NBTManager;
import org.bukkit.Bukkit;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.*;
import java.sql.*;
import java.util.Map;
import java.util.logging.Logger;

public class Main extends JavaPlugin implements Listener {
    private Logger logger;
    private NBTManager manager;
    private Connection connection;

    @Override
    public void onEnable() {
        logger = getLogger();
        saveDefaultConfig();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            FileConfiguration config = getConfig();
            String jdbcUrl = config.getString("url");
            String login = config.getString("username");
            String password = config.getString("password");
            connection = DriverManager.getConnection(jdbcUrl, login, password);
            try (PreparedStatement statement = connection.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS playerNbt (" +
                            "uuid VARCHAR(255) NOT NULL, " +
                            "nbt VARBINARY(4096) NOT NULL, " +
                            "PRIMARY KEY(uuid))")) {
                statement.execute();
            }

            manager = PowerNBT.getApi();
            Bukkit.getPluginManager().registerEvents(this, this);
        } catch (ClassNotFoundException e) {
            logger.warning("Mysql driver not found!");
            e.printStackTrace();
        } catch (SQLException e) {
            logger.warning("Can't connect to database!");
            e.printStackTrace();
        }
        logger.info("Enabled!");
    }

    @Override
    public void onDisable() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {
        try (PreparedStatement statement =
                     connection.prepareStatement("SELECT * FROM playerNbt WHERE uuid=?")) {
            statement.setString(1, event.getPlayer().getUniqueId().toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    try (ObjectInputStream ois = new ObjectInputStream(resultSet.getBinaryStream("nbt"))) {
                        Object object = ois.readObject();
                        if (object instanceof Map) {
                            NBTCompound playerNbt = new NBTCompound((Map) object);
                            manager.write(event.getPlayer(), playerNbt);
                        } else {
                            logger.warning("Failed to apply nbt on player: " + event.getPlayer().getName());
                        }
                    }
                }
            }
        } catch (SQLException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        NBTCompound playerData = manager.read(event.getPlayer());
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO playerNbt (uuid, nbt) " +
                        "VALUES (?, ?) " +
                        "ON DUPLICATE KEY UPDATE nbt = VALUES (nbt)");
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            statement.setString(1, event.getPlayer().getUniqueId().toString());
            oos.writeObject(playerData.toHashMap());

            try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray())) {
                statement.setBinaryStream(2, bais);
                statement.executeUpdate();
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
